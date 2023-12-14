#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import collections
import datetime
import inspect
import json
import os
import socket
import sys
import traceback
import uuid
from copy import deepcopy
from http.client import HTTPConnection
from itertools import chain
from subprocess import PIPE, Popen
from typing import Any
from urllib.parse import urlencode, urlparse

import jwt
import ujson

import dispel4py.base
import dispel4py.new.mappings
import dispel4py.new.processor
from dispel4py.base import NAME, IterativePE, SimpleFunctionPE
from dispel4py.core import GenericPE
from dispel4py.new import simple_process
from dispel4py.utils import make_hash
from dispel4py.workflow_graph import WorkflowGraph

INPUT_NAME = "input"
OUTPUT_DATA = "output"
OUTPUT_METADATA = "provenance"
"""@package docstring
Documentation for the dispe4py provenance module.

More details.
"""

SPROV_CL_D4PY_INPUT = {
    "mime-type": "text/json",
    "name": "dispel4py_input",
    "prov:type": "provone:Data",
    "value": "encoded_json",
}


def clean_empty(d):
    """
    Utility function that given a dictionary in input, removes all the properties that are set to None.
    It workes recursively through lists and nested documents

    """
    if not isinstance(d, (dict, list)):
        return d
    if isinstance(d, list):
        return [v for v in (clean_empty(v) for v in d) if v]
    return {k: v for k, v in ((k, clean_empty(v)) for k, v in d.items()) if v}


def total_size(o, handlers=None, verbose=False):
    """
    Returns the approximate memory footprint an object and all of its contents.

    Automatically finds the contents of the following builtin containers and
    their subclasses:  tuple, list, deque, dict, set and frozenset.
    To search other containers, add handlers to iterate over their contents:

    handlers = {SomeContainerClass: iter,
                OtherContainerClass: OtherContainerClass.get_elements}

    """
    if handlers is None:
        handlers = {}

    def dict_handler(d):
        return chain.from_iterable(d.items())

    all_handlers = {
        tuple: iter,
        list: iter,
        dict: dict_handler,
        set: iter,
        frozenset: iter,
    }
    # user handlers take precedence
    all_handlers.update(handlers)
    seen = set()
    # track which object id's have already been seen
    # estimate sizeof object without __sizeof__
    default_size = sys.getsizeof(0)

    def sizeof(o):
        if id(o) in seen:  # do not double count the same object
            return 0
        seen.add(id(o))
        s = sys.getsizeof(o, default_size)

        for typ, handler in all_handlers.items():
            if isinstance(o, typ):
                s += sum(map(sizeof, handler(o)))
                break
        return s

    return sizeof(o)


def write(self: GenericPE, name: str, data, **kwargs) -> None:
    """
    Redefines the native write function of the dispel4py SimpleFunctionPE to take into account
    provenance payload when transfering data.
    """
    if isinstance(data, dict) and "_d4p_prov" in data:
        data = data["_d4p_data"]

    self._write(name, data)


def _process(self, data):
    """
    Redefines the native _process function of the dispel4py SimpleFunctionPE to take into account
    provenance payload when accessing data for processing.
    """
    results = self.compute_fn(data, **self.params)
    if isinstance(results, dict) and "_d4p_prov" in results:
        if isinstance(self, (ProvenanceType)):
            return results
        else:
            return results["_d4p_data"]
    else:
        return results


def update_prov_state(self, *args, **kwargs) -> None:
    pass


dispel4py.base.GenericPE.update_prov_state = update_prov_state
dispel4py.base.SimpleFunctionPE.write = write
dispel4py.base.SimpleFunctionPE._process = _process


def getDestination_prov(self, data):
    """
    When provenance is activated it redefines the native dispel4py.new.process getDestination function to take into account provenance information
    when redirecting grouped operations.
    """
    if "TriggeredByProcessIterationID" in data[self.input_name]:
        output = tuple([data[self.input_name]["_d4p"][x] for x in self.group_by])
    else:
        try:
            output = tuple([data[self.input_name][x] for x in self.group_by])
        except Exception:
            output = ()
            print(data)

    dest_index = abs(make_hash(output)) % len(self.destinations)
    return [self.destinations[dest_index]]


def commandChain(commands, envhpc, queue=None):
    """
    Utility function to execute a chain of system commands on the hosting oeprating system.
    The current environment variable can be passed as parameter env.
    The queue parameter is used to store the stdoutdata, stderrdata of each process in message
    """
    stdoutdata, stderrdata = None, None
    for cmd in commands:
        print("Executing commandChain:" + str(cmd))
        process = Popen(cmd, stdout=PIPE, stderr=PIPE, env=envhpc, shell=True)
        stdoutdata, stderrdata = process.communicate()

    if queue is not None and stdoutdata is not None and stderrdata is not None:
        queue.put([stdoutdata, stderrdata])
        queue.close()
        return None
    else:
        return stdoutdata, stderrdata


def toW3Cprov(provenance, format="w3c-prov-json"):
    from prov.model import PROV, Namespace, ProvDocument

    g = ProvDocument()
    # Namespaces do not need to be explicitly added to a document
    vc = Namespace("verce", "http://verce.eu")
    g.add_namespace("dcterms", "http://purl.org/dc/terms/")

    # Specifing user
    # First time the ex namespace was used, it is added to the document
    # automatically
    g.agent(
        vc["ag_" + provenance["username"]],
        other_attributes={"dcterms:author": provenance["username"]},
    )

    # Specify bundle

    if provenance["type"] == "workflow_run":
        provenance.update({"runId": provenance["_id"]})
        dic = {}
        i = 0

        for key in provenance:
            if key != "input":
                if ":" in key:
                    dic.update({key: provenance[key]})
                else:
                    dic.update({vc[key]: provenance[key]})

        dic.update({"prov:type": PROV["Bundle"]})
        g.entity(vc[provenance["runId"]], dic)
    else:
        g.entity(vc[provenance["runId"]], {"prov:type": PROV["Bundle"]})

    g.wasAttributedTo(
        vc[provenance["runId"]],
        vc["ag_" + provenance["username"]],
        identifier=vc["run_" + provenance["runId"]],
    )
    bundle = g.bundle(vc[provenance["runId"]])

    # Specifing creator of the activity (to be collected from the registy)

    if "creator" in provenance:
        # First time the ex namespace was used, it is added to the document
        # automatically
        bundle.agent(
            vc[f"ag_{provenance['creator']}"],
            other_attributes={"dcterms:creator": provenance["creator"]},
        )
        bundle.was_associated_with(
            f"process_{provenance['_id']}{vc['ag_' + provenance['creator']]}",
        )
        bundle.was_attributed_to(
            vc[provenance["runId"]],
            vc[f"ag_{provenance['creator']}"],
        )

    # Check for workflow input entities
    if provenance["type"] == "workflow_run":
        dic = {}
        i = 0
        if not isinstance(provenance["input"], list):
            provenance["input"] = [provenance["input"]]
            for y in provenance["input"]:
                for key in y:
                    if ":" in key:
                        dic.update({key: y[key]})
                    else:
                        dic.update({vc[key]: y[key]})
            dic.update({"prov:type": "worklfow_input"})
            bundle.entity(vc[f"data_{provenance['_id']}_{i}"], dic)
            bundle.was_generated_by(
                vc[f"data_{provenance['_id']}_{i}"],
                identifier=vc[f"wgb_{provenance['_id']}_{i}"],
            )

            i += 1
        if format == "w3c-prov-xml":
            return str(g.serialize(format="xml"))
        else:
            return json.loads(g.serialize(indent=4))

    # Adding activity information for lineage
    dic = {}
    for key in provenance:
        if not isinstance(provenance[key], list):
            if ":" in key:
                dic.update({key: provenance[key]})
            else:
                if key == "location":
                    dic.update({"prov:atLocation": provenance[key]})
                else:
                    dic.update({vc[key]: provenance[key]})

    bundle.activity(
        vc[f"process_{provenance['_id']}"],
        provenance["startTime"],
        provenance["endTime"],
        dic.update({"prov:type": provenance["name"]}),
    )

    # Adding parameters to the document as input entities
    dic = {}
    for x in provenance["parameters"]:
        if ":" in x["key"]:
            dic.update({x["key"]: x["val"]})
        else:
            dic.update({vc[x["key"]]: x["val"]})

    dic.update({"prov:type": "parameters"})
    bundle.entity(vc[f"parameters_{provenance['_id']}"], dic)
    bundle.used(
        vc[f"process_{provenance['_id']}"],
        vc[f"parameters_{provenance['_id']}"],
        identifier=vc[f"used_{provenance['_id']}"],
    )

    # Adding entities to the document as output metadata
    for x in provenance["streams"]:
        i = 0
        parent_dic = {}
        for key in x:
            if key == "location":
                parent_dic.update({"prov:atLocation": str(x[key])})
            else:
                parent_dic.update({vc[key]: str(x[key])})

        c1 = bundle.collection(vc[x["id"]], other_attributes=parent_dic)
        bundle.was_generated_by(
            vc[x["id"]],
            vc[f"process_{provenance['_id']}"],
            identifier=vc[f"wgb_{x['id']}"],
        )

        for d in provenance["derivationIds"]:
            bundle.was_derived_from(
                vc[x["id"]],
                vc[d["DerivedFromDatasetID"]],
                identifier=vc[f"wdf_{x['id']}"],
            )

        for y in x["content"]:
            dic = {}

            if isinstance(y, dict):
                val = None
                for key in y:
                    try:
                        val = num(y[key])

                    except Exception:
                        val = str(y[key])

                    if ":" in key:
                        dic.update({key: val})
                    else:
                        dic.update({vc[key]: val})
            else:
                dic = {vc["text"]: y}

            dic.update({"verce:parent_entity": vc[f"data_{x['id']}"]})
            e1 = bundle.entity(vc[f"data_{x['id']}_{i}"], dic)

            bundle.had_member(c1, e1)
            bundle.was_generated_by(
                vc[f"data_{x['id']}_{i}"],
                vc[f"process_{provenance['_id']}"],
                identifier=vc[f"wgb_{x['id']}_{i}"],
            )

            for d in provenance["derivationIds"]:
                bundle.was_derived_from(
                    vc[f"data_{x['id']}_{i}"],
                    vc[d["DerivedFromDatasetID"]],
                    identifier=vc[f"wdf_data_{x['id']}_{i}"],
                )

            i += 1

    if format == "w3c-prov-xml":
        return str(g.serialize(format="xml"))
    else:
        return json.loads(g.serialize(indent=4))


def getUniqueId(data=None):
    if data is None:
        return f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid1()}"
    else:
        print(f"ID: {id(data)} Data: {data}")
        return f"{socket.gethostname()}-{os.getpid()}-{self.instanceId}-{id(data)}"


def num(s: Any):
    try:
        return int(s)
    except Exception:
        return float(s)


_d4p_plan_sqn = 0


class ProvenanceType(GenericPE):
    """
    A workflow is a program that combines atomic and independent processing elements
    via a specification language and a library of components. More advanced systems
    adopt abstractions to facilitate re-use of workflows across users'' contexts and application
    domains. While methods can be multi-disciplinary, provenance
    should be meaningful to the domain adopting them. Therefore, a portable specification
    of a workflow requires mechanisms allowing the contextualisation of the provenance
    produced. For instance, users may want to extract domain-metadata from a component
    or groups of components adopting vocabularies that match their domain and current
    research, tuning the level of granularity. To allow this level of flexibility, we explore
    an approach that considers a workflow component described by a class, according to
    the Object-Oriented paradigm. The class defines the behaviour of its instances as their
    type, which specifies what an instance will do in terms of a set of methods. We introduce
    the concept of _ProvenanceType_, that augments the basic behaviour by extending
    the class native type, so that a subset of those methods perform the additional actions
    needed to deliver provenance data. Some of these are being used by some of the preexisting
    methods, and characterise the behaviour of the specific provenance type, some
    others can be used by the developer to easily control precision and granularity. This approach,
    tries to balance between automation, transparency and explicit intervention of the developer of a data-intensive tool, who
    can tune provenance-awareness through easy-to-use extensions.

    The type-based approach to provenance collection provides a generic _ProvenanceType_ class
    that defines the properties of a provenance-aware workflow component. It provides
    a wrapper that meets the provenance requirements, while leaving the computational
    behaviour of the component unchanged. Types may be developed as __Pattern Type__ and __Contextual Type__ to represent respectively complex
    computational patterns and to capture specific metadata contextualisations associated to the produce output data.

    The _ProvenanceType_ presents the following class constants to indicate where the lineage information will be stored. Options include a remote
    repository, a local file system or a _ProvenanceSensor_ (experimental).

    - _SAVE_MODE_SERVICE='service'_
    - _SAVE_MODE_FILE='file'_
    - _SAVE_MODE_SENSOR='sensor'_

    The following variables will be used to configure some general provenance capturing properties

    - _PROV_PATH_: When _SAVE_MODE_SERVICE_ is chosen, this variable should be populated with a string indicating a file system path where the lineage will be stored.
    - _REPOS_URL_: When _SAVE_MODE_SERVICE_ is chosen, this variable should be populated with a string indicating the repository endpoint (S-ProvFlow) where the provenance will be sent.
    - _PROV_EXPORT_URL: The service endpoint from where the provenance of a workflow execution, after being stored, can be extracted in PROV format.
    - _BULK_SIZE_: Number of lineage documents to be stored in a single file or in a single request to the remote service. Helps tuning the overhead brough by the latency of accessing storage resources.

    """

    PROV_PATH = "./"

    REPOS_URL = ""
    PROV_EXPORT_URL = ""
    PROV_BEARER_TOKEN = ""

    SAVE_MODE_SERVICE = "service"
    SAVE_MODE_FILE = "file"
    SAVE_MODE_SENSOR = "sensor"
    BULK_SIZE = 1

    send_prov_to_sensor = False

    def getProvStateObjectId(self, name):
        """
        Return the id of a named object stored in the provenance state
        """
        if name in self.stateCollection:
            return self.stateCollection[name]
        else:
            return None

    def makeProcessId(self, **kwargs):
        return f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid1()}"

    def makeUniqueId(self, data, port):
        return f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid1()}"

    def _updateState(self, name, id):
        if name in self.stateCollection:
            self.stateCollectionId.remove(self.stateCollection[name])
        self.stateCollection[name] = id
        self.stateCollectionId.append(id)

    def getUniqueId(self, data, port, **kwargs):
        data_id = self.makeUniqueId(data, port)
        if "name" in kwargs:
            self._updateState(kwargs["name"], data_id)

        return data_id

    def apply_derivation_rule(
        self,
        event,
        voidInvocation,
        oport=None,
        iport=None,
        data=None,
        metadata=None,
    ):
        """
        In support of the implementation of a _ProvenanceType_ realising a lineage _Pattern type_. This method is invoked by the _ProvenanceType_ each iteration when a decision has to be made whether to ignore or discard the dependencies on the ingested stream
        and stateful entities, applying a specific provenance pattern, thereby creating input/output derivations. The framework invokes this method every time the data is written on an output port (_event_: _write_) and every
        time an invocation (_s-prov:Invocation_) ends (_event_: _end_invocation_event_). The latter can be further described by  the boolean parameter _voidInvocation_, indicating whether the invocation terminated with any data produced.
        The default implementation provides a _stateless_ behaviour, where the output depends only from the input data recieved during the invocation.

        """
        if (event == "end_invocation_event") and voidInvocation:
            self.discardInFlow(discardState=True)

        if (event == "end_invocation_event") and not voidInvocation:
            self.discardInFlow(discardState=True)

    def pe_init(self, *args, **kwargs):
        global _d4p_plan_sqn
        self._add_input("_d4py_feedback", grouping="all")
        self.stateCollection = {}
        self.stateCollectionId = []
        self.impcls = None
        self.bulk_prov = []
        self.stateful = False
        self.stateDerivations = []

        if "pe_class" in kwargs and kwargs["pe_class"] != GenericPE:
            self.impcls = kwargs["pe_class"]

        if (
            "sel_rules" in kwargs
            and kwargs["sel_rules"] is not None
            and self.name in kwargs["sel_rules"]
        ):
            print(self.name + " " + str(kwargs["sel_rules"][self.name]))
            self.sel_rules = kwargs["sel_rules"][self.name]
        else:
            self.sel_rules = None

        if "transfer_rules" in kwargs and self.name in kwargs["transfer_rules"]:
            print(f"{self.name} {kwargs['transfer_rules'][self.name]}")
            self.transfer_rules = kwargs["transfer_rules"][self.name]
        else:
            self.transfer_rules = None

        self.creator = kwargs.get("creator", None)

        self.error = ""

        if not hasattr(self, "parameters"):
            self.parameters = {}
        if not hasattr(self, "controlParameters"):
            self.controlParameters = {}

        if "controlParameters" in kwargs:
            self.controlParameters = kwargs["controlParameters"]

        out_md = {}
        out_md[NAME] = OUTPUT_METADATA

        self._add_output(OUTPUT_METADATA)
        self.taskId = str(uuid.uuid1())

        self.provon = True

        self.save_mode = kwargs.get("save_mode", ProvenanceType.SAVE_MODE_FILE)

        self.wcount = 0
        self.resetflow = False
        self.stateUpdateIndex = 0
        self.ignore_inputs = False
        self.ignore_state = False
        self.ignore_past_flow = False
        self.derivationIds = []
        self.iterationIndex = 0

        _d4p_plan_sqn = _d4p_plan_sqn + 1
        self.countstatewrite = 0
        if not hasattr(self, "comp_id"):
            self.behalfOf = self.id
        else:
            self.behalfOf = self.comp_id
        if not hasattr(self, "prov_cluster"):
            self.prov_cluster = self.behalfOf

    def __init__(self):
        GenericPE.__init__(self)
        self.parameters = {}
        self._add_output(OUTPUT_METADATA)
        self.ns = {}

    def __getUniqueId(self):
        return f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid1()}"

    def getDataStreams(self, inputs):
        streams = {}
        for inp in self.inputconnections:
            if inp not in inputs:
                continue
            values = inputs[inp]
            data = values[0:] if isinstance(values, list) else values
            streams["streams"].update({inp: data})
        return streams

    def getInputAt(self, port="input", index=None):
        """
        Return input data currently available at a specific _port_. When reading input of a grouped operator, the _gindex_ parameter allows to access exclusively the data related to the group index.
        """
        if index is None:
            return self.inputs[port]
        else:
            return self.inputs[port][index]

    def _preprocess(self):
        self.instanceId = f"{self.name}-Instance--{self.makeProcessId()}"

        super()._preprocess()

    "This method must be implemented in the original PE"
    "to handle prov feedback"

    # def process_feedback(self,data):
    #    self.log("NO Feedback procedure implemented")

    def process_feedback(self, feedback):
        self.feedbackIteration = True
        self._process_feedback(feedback)

    def process(self, inputs):
        self.feedbackIteration = False
        self.void_invocation = True
        self.iterationIndex += 1

        if "_d4py_feedback" in inputs:
            "state could be used here to track the occurring changes"
            self.process_feedback(inputs["_d4py_feedback"])
        else:
            self.__processwrapper(inputs)

        for x in inputs:
            data = inputs[x]
            if type(data) == dict and "_d4p" in data:
                self.apply_derivation_rule(
                    "end_invocation_event",
                    self.void_invocation,
                    iport=x,
                    data=data["_d4p"],
                )
            else:
                self.apply_derivation_rule(
                    "end_invocation_event",
                    self.void_invocation,
                    iport=x,
                    data=data,
                )

    def addNamespacePrefix(self, prefix, url):
        """
        In support of the implementation of a _ProvenanceType_ realising a lineage _Contextualisation type_.
        A Namespace _prefix_ can be declared with its vocabulary _url_ to map the metadata terms to external controlled vocabularies.
        They can be used to qualify the metadata terms extracted from the _extractItemMetadata_ function,
        as well as for those terms injected selectively at runtime by the _write_ method. The namespaces will be used
        consistently when exporting the lineage traces to semantic-web formats, such as RDF.
        """
        self.ns.update({prefix: url})

    def extractItemMetadata(self, data, port):
        """
        In support of the implementation of a _ProvenanceType_ realising a lineage _Contextualisation type_.
        Extracts metadata from the domain specific content of the data (s-prov:DataGranules) written on a components output _port_, according to a particular vocabulary.
        """
        return {}

    def preprocess(self):
        if self.save_mode == ProvenanceType.SAVE_MODE_SERVICE:
            self.provurl = urlparse(ProvenanceType.REPOS_URL)
        self._preprocess()

    def postprocess(self):
        if len(self.bulk_prov) > 0:
            if self.save_mode == ProvenanceType.SAVE_MODE_SERVICE:
                response = self.sendProvRequestToService()
                self.log(
                    f"Postprocess: {(response.status, response.reason, response.read())}",
                )
                self.connection.close()
                self.bulk_prov[:] = []
            elif self.save_mode == ProvenanceType.SAVE_MODE_FILE:
                filep = open(
                    ProvenanceType.PROV_PATH + "/bulk_" + self.makeProcessId(),
                    "w",
                )
                ujson.dump(self.bulk_prov, filep)
            elif self.save_mode == ProvenanceType.SAVE_MODE_SENSOR:
                super().write(
                    OUTPUT_METADATA,
                    {
                        "prov_cluster": self.prov_cluster,
                        "provenance": deepcopy(self.bulk_prov),
                    },
                )

        self._postprocess()

    def sendProvToSensor(self, prov):
        self.bulk_prov.append(deepcopy(prov))

        if len(self.bulk_prov) == ProvenanceType.BULK_SIZE:
            super().write(
                OUTPUT_METADATA,
                {
                    "prov_cluster": self.prov_cluster,
                    "provenance": deepcopy(self.bulk_prov),
                },
            )

            self.bulk_prov[:] = []

    def sendProvToService(self, prov):
        if isinstance(prov, list) and "data" in prov[0]:
            prov = prov[0]["data"]

        self.bulk_prov.append(deepcopy(prov))

        if len(self.bulk_prov) > ProvenanceType.BULK_SIZE:
            self.sendProvRequestToService()
            self.bulk_prov[:] = []

    def sendProvRequestToService(self):
        params = urlencode(
            {
                "prov": ujson.dumps(
                    self.bulk_prov,
                    encode_html_chars=True,
                    reject_bytes=False,
                ),
            },
        )
        headers = {
            "Content-type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        }
        if self.PROV_BEARER_TOKEN:
            # Check if token is expired and add to the HTTP headers if it isn't.
            try:
                jwt.decode(
                    self.PROV_BEARER_TOKEN,
                    options={"verify_signature": False, "verify_aud": False},
                    verify_expiration=True,
                )
            except jwt.exceptions.ExpiredSignatureError:
                self.log(
                    "Token expired. We can only refresh if we talk directly to sprov-api.",
                )
            except jwt.exceptions.DecodeError:
                self.log(
                    "Malformed token supplied. It must be a properly formatted JWT!",
                )

            headers["Authorization"] = f"Bearer {self.PROV_BEARER_TOKEN}"

        self.connection = HTTPConnection(self.provurl.netloc)
        self.connection.request("POST", self.provurl.path, params, headers)
        return self.connection.getresponse()

    def writeProvToFile(self, prov):
        if isinstance(prov, list) and "data" in prov[0]:
            prov = prov[0]["data"]

        self.bulk_prov.append(prov)

        if len(self.bulk_prov) == ProvenanceType.BULK_SIZE:
            filep = open(
                ProvenanceType.PROV_PATH + "/bulk_" + self.makeProcessId(),
                "w",
            )
            ujson.dump(self.bulk_prov, filep)
            self.bulk_prov[:] = []

    def flushData(self, data, metadata, port, **kwargs):
        trace = {}
        stream = data
        try:
            if self.provon:
                self.endTime = datetime.datetime.utcnow()
                trace = self.packageAll(metadata)

            stream = self.prepareOutputStream(data, trace, port, **kwargs)

            try:
                if port is not None and port != "_d4p_state" and port != "error":
                    super().write(port, stream)

            except:
                self.log(traceback.format_exc())
                # If cant write doesn't matter move on
            try:
                if self.provon:
                    if (ProvenanceType.send_prov_to_sensor is True) or (
                        self.save_mode == ProvenanceType.SAVE_MODE_SENSOR
                    ):
                        self.sendProvToSensor(trace["metadata"])

                    if self.save_mode == ProvenanceType.SAVE_MODE_SERVICE:
                        self.sendProvToService(trace["metadata"])
                    if self.save_mode == ProvenanceType.SAVE_MODE_FILE:
                        self.writeProvToFile(trace["metadata"])

            except:
                self.log(traceback.format_exc())
                # If cant write doesn't matter move on

            return True

        except Exception:
            self.log(traceback.format_exc())
            if self.provon:
                self.error += f" FlushChunk Error: {traceback.format_exc()}"

    def __processwrapper(self, data):
        try:
            self.initParameters()

            self.inputs = self.importInputData(data)
            return self.__computewrapper(self.inputs)

        except:
            self.log(traceback.format_exc())

    def initParameters(self):
        self.error = ""
        self.w3c_prov = {}
        self.inMetaStreams = None
        self.username = None
        self.runId = None
        self.mapping = "simple"

        try:
            if "username" in self.controlParameters:
                self.username = self.controlParameters["username"]
            if "runId" in self.controlParameters:
                self.runId = self.controlParameters["runId"]

        except:
            self.runId = ""

        self.outputdest = (
            self.controlParameters["outputdest"]
            if "outputdest" in self.controlParameters
            else "None"
        )
        self.rootpath = (
            self.controlParameters["inputrootpath"]
            if "inputrootpath" in self.controlParameters
            else "None"
        )
        self.outputid = (
            self.controlParameters["outputid"]
            if "outputid" in self.controlParameters
            else "None"
        )

    def importInputData(self, data):
        inputs = {}

        try:
            if not isinstance(data, collections.Iterable):
                return data
            else:
                for x in data:
                    self.buildDerivation(data[x], port=x)
                    if type(data[x]) == dict and "_d4p" in data[x]:
                        inputs[x] = data[x]["_d4p"]
                    else:
                        inputs[x] = data[x]
                return inputs

        except Exception:
            self.output = ""
            self.error += f"Reading Input Error: {traceback.format_exc()}"
            raise

    def writeResults(self, name, result):
        self.apply_derivation_rule("write", True, data=result, oport=name)
        self.void_invocation = False

        if isinstance(result, dict) and "_d4p_prov" in result:
            meta = result["_d4p_prov"]
            result = result["_d4p_data"]

            if "error" in meta:
                self.extractProvenance(result, output_port=name, **meta)
            else:
                self.extractProvenance(
                    result,
                    error=self.error,
                    output_port=name,
                    **meta,
                )

        else:
            self.extractProvenance(result, error=self.error, output_port=name)

    def __markIteration(self):
        self.startTime = datetime.datetime.utcnow()
        self.iterationId = f"{self.name}-{self.makeProcessId()}"

    def __computewrapper(self, inputs):
        try:
            result = None

            self.__markIteration()

            if self.impcls is not None and isinstance(self, self.impcls):
                try:
                    if hasattr(self, "params"):
                        self.parameters = deepcopy(self.params)
                    result = self._process(inputs[self.impcls.INPUT_NAME])
                    if result is not None:
                        self.log(self.impcls)
                        self.writeResults(self.impcls.OUTPUT_NAME, result)
                        result = None
                except:
                    traceback.format_exc()
                    result = self._process(inputs)

            else:
                result = self._process(inputs)

            if result is not None:
                for x in result:
                    self.writeResults(x, result[x])

        except Exception:
            self.log(f" Compute Error: {traceback.format_exc()}")
            self.error += f" Compute Error: {traceback.format_exc()}"
            self.writeResults("error", {"error": "null"})

    def prepareOutputStream(self, data, trace, port, **kwargs):
        try:
            streamtransfer = {}
            streamtransfer["_d4p"] = data

            try:
                streamtransfer["prov_cluster"] = self.prov_cluster
                streamtransfer["port"] = port

                if self.provon:
                    streamtransfer["id"] = trace["metadata"]["streams"][0]["id"]
                    streamtransfer["TriggeredByProcessIterationID"] = self.iterationId

                    if port == "_d4p_state":
                        self._updateState(
                            kwargs["lookupterm"],
                            trace["metadata"]["streams"][0]["id"],
                        )
                        streamtransfer["lookupterm"] = kwargs["lookupterm"]
                        self.buildDerivation(streamtransfer, port="_d4p_state")

                else:
                    streamtransfer["id"] = self.derivationIds[0]["DerivedFromDatasetID"]
                    streamtransfer[
                        "TriggeredByProcessIterationID"
                    ] = self.derivationIds[0]["TriggeredByProcessIterationID"]
                    streamtransfer["up:assertionType"] = "up:Incomplete"
            except Exception:
                pass
            return streamtransfer

        except Exception:
            self.error += self.name + f" Writing output Error: {traceback.format_exc()}"
            raise

    def ignorePastFlow(self):
        """
        In support of the implementation of a _ProvenanceType_ realising a lineage __Pattern type__.

        It instructs the type to ignore the all the inputs when the method _apply_derivation_rule_ is invoked for a certain event."
        """
        self.ignore_past_flow = True

    def ignoreState(self):
        """
        In support of the implementation of a _ProvenanceType_ realising a lineage __Pattern type__.

        It instructs the type to ignore the content of the provenance state when the method _apply_derivation_rule_ is invoked for a certain event."
        """
        self.ignore_state = True

    def packageAll(self, contentmeta):
        metadata = {}
        if self.provon:
            try:
                # Identifies the actual iteration over the instance
                metadata.update(
                    {
                        "iterationId": self.iterationId,
                        # Identifies the actual writing process'
                        "actedOnBehalfOf": self.behalfOf,
                        "_id": f"{self.id}_write_{self.makeProcessId()}",
                        "iterationIndex": self.iterationIndex,
                        "instanceId": self.instanceId,
                        "annotations": {},
                    },
                )

                if self.feedbackIteration:
                    metadata.update(
                        {"_id": f"{self.id}_feedback_{self.makeProcessId()}"},
                    )
                elif self.stateful:
                    metadata.update(
                        {"_id": f"{self.id}_stateful_{self.makeProcessId()}"},
                    )

                else:
                    metadata.update({"_id": f"{self.id}_write_{self.makeProcessId()}"})

                metadata.update(
                    {
                        "stateful": not self.resetflow,
                        "feedbackIteration": self.feedbackIteration,
                        "worker": socket.gethostname(),
                        "parameters": self.parameters,
                        "errors": self.error,
                        "pid": str(os.getpid()),
                    },
                )

                if self.ignore_inputs is True:
                    derivations = [
                        x
                        for x in self.derivationIds
                        if x["port"] == "_d4p_state"
                        and x["DerivedFromDatasetID"] in self.stateCollectionId
                    ]
                    metadata.update({"derivationIds": derivations})
                    self.ignore_inputs = False

                elif self.ignore_past_flow is True:
                    derivations = [
                        x
                        for x in self.derivationIds
                        if (
                            x["iterationIndex"] == self.iterationIndex
                            or x["port"] == "_d4p_state"
                        )
                    ]
                    metadata.update({"derivationIds": derivations})
                elif self.ignore_state is True:
                    derivations = [
                        x for x in self.derivationIds if x["port"] != "_d4p_state"
                    ]
                    metadata.update({"derivationIds": derivations})
                else:
                    metadata.update({"derivationIds": self.derivationIds})
                    self.ignore_past_flow = False

                metadata.update(
                    {
                        "name": self.name,
                        "runId": self.runId,
                        "username": self.username,
                        "startTime": str(self.startTime),
                        "endTime": str(self.endTime),
                        "type": "lineage",
                        "streams": contentmeta,
                        "mapping": self.mapping,
                    },
                )

                if hasattr(self, "prov_cluster"):
                    metadata.update({"prov_cluster": self.prov_cluster})

                if self.creator is not None:
                    metadata.update({"creator": self.creator})
            except Exception:
                self.error += f" Packaging Error: {traceback.format_exc()}"
                self.log(traceback.format_exc())

        return {
            "metadata": metadata,
            "error": self.error,
        }

    """
    Imports Input metadata if available, the metadata will be
    available in the self.inMetaStreams property as a Dictionary
    """

    def __importInputMetadata(self):
        try:
            self.inMetaStreams = self.input["metadata"]["streams"]
        except Exception:
            None

    """
    TBD: Produces a bulk output with data,location,format,metadata:
    to be used in exclusion of
    self.streamItemsLocations
    self.streamItemsFormat
    self.outputstreams
    """

    def discardState(self):
        """
        In support of the implementation of a _ProvenanceType_ realising a lineage __Pattern type__.

        It instructs the type to reset the data dependencies in the provenance state when the method _apply_derivation_rule_ is invoked for a certain event.
        These will not be availabe in the following invocations."
        """

        derivations = [x for x in self.derivationIds if x["port"] != "_d4p_state"]

        self.derivationIds = derivations

    def discardInFlow(self, wlength=None, discardState=False):
        """
        In support of the implementation of a _ProvenanceType_ realising a lineage __Pattern type__.

        It instructs the type to reset the data dependencies related to the component''s inputs when the method _apply_derivation_rule_ is invoked for a certain event.
        These will not be availabe in the following invocations."
        """

        if discardState is True:
            if wlength is None:
                self.derivationIds = []
            else:
                count = 0
                for x in self.derivationIds:
                    if (
                        x is not None
                        and x["port"] != "_d4p_state"
                        and count >= wlength - 1
                    ):
                        self.derivationIds.remove(x)
                    count += 1
                for x in self.derivationIds:
                    if x is not None and x["port"] == "_d4p_state":
                        self.derivationIds.remove(x)

        else:
            maxit = 0
            state = None
            for x in self.derivationIds:
                if (
                    x is not None
                    and x["port"] == "_d4p_state"
                    and x["iterationIndex"] >= maxit
                ):
                    state = x
                    maxit = x["iterationIndex"]

            if wlength is None:
                if state is not None:
                    self.derivationIds = [state]
                else:
                    self.derivationIds = []
            else:
                count = 0
                for x in self.derivationIds:
                    # self.log("COUNT: "+str(count)+" WLENTGH: "+str(wlength))
                    if (
                        x is not None
                        and x["port"] != "_d4p_state"
                        and count >= wlength - 1
                    ):
                        # self.log("REMOVE: "+str(x['iterationIndex']))
                        del self.derivationIds[0]
                    count += 1

                if state is not None:
                    self.derivationIds.append(state)

    def update_prov_state(
        self,
        lookupterm,
        data,
        location="",
        format="",
        metadata=None,
        ignore_inputs=False,
        ignore_state=True,
        stateless=False,
        **kwargs,
    ):
        """
        In support of the implementation of a _ProvenanceType_ realising a lineage _Pattern type_ or inn those circumstances where developers require to explicitly manage the provenance information within the component''s logic,.

        Updates the provenance state (_s-prov:StateCollection_) with a reference, identified by a _lookupterm_, to a new _data_ entity or to the current input. The _lookupterm_ will allow developers to refer to the entity when this is used to derive new data.
        Developers can specify additional _medatata_ by passing a metadata dictionary. This will enrich the one generated by the _extractItemMetadata_ method.
        Optionally the can also specify _format_ and _location_ of the output when this is a concrete resource (file, db entry, online url), as well as instructing the provenance generation to 'ignore_input' and 'ignore_state' dependencies.

        The _kwargs_ parameter allows to pass an argument _dep_ where developers can specify a list of data _id_ to explicitly declare dependencies with any data in the provenance state (_s-prov:StateCollection_).
        """
        if metadata is None:
            metadata = {}
        self.endTime = datetime.datetime.utcnow()
        self.stateful = True
        self.ignore_inputs = ignore_inputs
        self.ignore_state = ignore_state
        self.addprov = True
        kwargs["lookupterm"] = lookupterm
        if self.provon:
            self.log(type(data))
            if data is None:
                self._updateState(
                    lookupterm,
                    self.derivationIds[len(self.derivationIds) - 1][
                        "DerivedFromDatasetID"
                    ],
                )

            else:
                if "dep" in kwargs and kwargs["dep"] is not None:
                    for d in kwargs["dep"]:
                        did = self.getProvStateObjectId(d)

                        if did is not None:
                            self.buildDerivation(
                                {
                                    "id": did,
                                    "TriggeredByProcessIterationID": self.iterationId,
                                    "prov_cluster": self.prov_cluster,
                                    "lookupterm": d,
                                },
                                port="_d4p_state",
                            )
                self.extractProvenance(
                    data,
                    location,
                    format,
                    metadata,
                    output_port="_d4p_state",
                    **kwargs,
                )

        self.ignore_inputs = False
        self.ignore_state = False

        if "dep" in kwargs and kwargs["dep"] is not None:
            for d in kwargs["dep"]:
                self.removeDerivation(name=d)

        self.stateful = False

    def extractProvenance(
        self,
        data,
        location="",
        format="",
        metadata=None,
        control=None,
        attributes=None,
        error="",
        output_port="",
        **kwargs,
    ):
        if attributes is None:
            attributes = {}
        if control is None:
            control = {}
        if metadata is None:
            metadata = {}
        self.error = error

        if metadata is None:
            metadata = {}
        elif isinstance(metadata, list):
            metadata.append(attributes)
        else:
            metadata.update(attributes)

        usermeta = {}

        if "s-prov:skip" in control and bool(control["s-prov:skip"]):
            self.provon = False
        else:
            self.provon = True
            usermeta = self.buildUserMetadata(
                data,
                location=location,
                format=format,
                metadata=metadata,
                control=control,
                attributes=attributes,
                error=error,
                output_port=output_port,
                **kwargs,
            )

        self.flushData(data, usermeta, output_port, **kwargs)

        return usermeta

    """
    Overrides the GenericPE write inclduing options such as:
    metadata: is the dictionary of metadata describing the data.
    format: typically contains the mime-type of the data.
    errors: users may identify erroneous situations and classify and describe
    them using this parameter.
    control: are the control instructions like s-prov:skip and s-prov:immediateAccess
    respectively selectively producing traces for the data stream
    passing through the component and trigger- ing data transfer
    operations for the specific intermediate ele- ment, towards
    an external target resource.
    state-reset: triggers the reset of the dependencies for the next iteration.
    eg. state-reset=False will produce a stateful iteration.
    Default is True
    """

    def write(self, name, data, **kwargs):
        """
        This is the native write operation of dispel4py triggering the transfer of data between adjacent
        components of a workflow. It is extended by the _ProvenanceType_ with explicit provenance
        controls through the _kwargs_ parameter. We assume these to be ignored
        when provenance is deactivated. Also this method can use the lookup tags to
        establish dependencies of output data on entities in the provenance state.

        The _kwargs_ parameter allows to pass the following arguments:
        - _dep_ : developers can specify a list of data _id_ to explicitly declare dependencies with any data in the provenance state (_s-prov:StateCollection_).
        - _metadata_: developers can specify additional medatata by passing a metadata dictionary.
        - _ignore_inputs_: instructs the provenance generation to ignore the dependencies on the current inputs.
        - _format_: the format of the output.
        - _location_: location of the output when this is a concrete resource (file, db entry, online url).
        """
        self.void_invocation = False

        iport = None

        for i in self.inputs:
            iport = i

        if "metadata" in kwargs:
            self.apply_derivation_rule(
                "write",
                True,
                oport=name,
                iport=iport,
                data=data,
                metadata=kwargs["metadata"],
            )
        else:
            self.apply_derivation_rule(
                "write",
                True,
                oport=name,
                iport=iport,
                data=data,
            )

        self.endTime = datetime.datetime.utcnow()

        if "dep" in kwargs and kwargs["dep"] is not None:
            for d in kwargs["dep"]:
                self.buildDerivation(
                    {
                        "id": self.getProvStateObjectId(d),
                        "TriggeredByProcessIterationID": self.iterationId,
                        "prov_cluster": self.prov_cluster,
                        "lookupterm": d,
                    },
                    port="_d4p_state",
                )
        elif len(self.stateDerivations) > 0:
            for d in self.stateDerivations:
                self.buildDerivation(
                    {
                        "id": self.getProvStateObjectId(d),
                        "TriggeredByProcessIterationID": self.iterationId,
                        "prov_cluster": self.prov_cluster,
                        "lookupterm": d,
                    },
                    port="_d4p_state",
                )

        if "ignore_inputs" in kwargs:
            self.ignore_inputs = kwargs["ignore_inputs"]

        self.extractProvenance(data, output_port=name, **kwargs)

        if "dep" in kwargs and kwargs["dep"] is not None:
            for d in kwargs["dep"]:
                self.removeDerivation(name=d)
        elif len(self.stateDerivations) > 0:
            for d in self.stateDerivations:
                self.removeDerivation(name=d)

        self.stateDerivations = []

    def setStateDerivations(self, terms):
        self.stateDerivations = terms

    def checkSelectiveRule(self, streammeta):
        """
        In alignement with what was previously specified in the configure_prov_run for the Processing Element,
        check the data granule metadata whether its properies values fall in a selective provenance generation rule.
        """
        self.log(f"Checking Selectivity-Rules: {self.sel_rules}")
        rules = self.sel_rules["rules"]

        for key in rules:
            for s in streammeta:
                if key in s:
                    self.log(s[key])

                    if "$eq" in rules[key] and s[key] == rules[key]["$eq"]:
                        return True
                    elif "$gt" in rules[key] and "$lt" in rules[key]:
                        return bool(
                            s[key] > rules[key]["$gt"]
                            and type(s[key]) is not list
                            and s[key] < rules[key]["$lt"],
                        )
                    elif (
                        "$gt" in rules[key]
                        and type(s[key]) is not list
                        and s[key] > rules[key]["$gt"]
                    ):
                        return True
                    return bool(
                        "$lt" in rules[key]
                        and type(s[key]) is not list
                        and s[key] < rules[key]["$lt"],
                    )
        return self.provon

    def checkTransferRule(self, streammeta):
        """
        In alignement with what was previously specified in the configure_prov_run for the Processing Element,
        check the data granule metadata whether its properies values fall in a selective data transfer rule.
        """
        self.log("Checking Transfer-Rules")
        for key in self.transfer_rules["rules"]:
            for s in streammeta:
                if key in s:
                    self.log(s[key])
                    self.log(type(s[key]))

                    if (
                        "$eq" in self.transfer_rules["rules"][key]
                        and s[key] == self.transfer_rules["rules"][key]["$eq"]
                    ):
                        return True
                    elif (
                        "$gt" in self.transfer_rules["rules"][key]
                        and "$lt" in self.transfer_rules["rules"][key]
                    ):
                        if (
                            s[key] > self.transfer_rules["rules"][key]["$gt"]
                            and s[key] < self.transfer_rules["rules"][key]["$lt"]
                        ):
                            self.log("GT-LT")

                            return True
                    elif (
                        "$gt" in self.transfer_rules["rules"][key]
                        and s[key] > self.transfer_rules["rules"][key]["$gt"]
                    ):
                        self.log("GT")

                        return True
                    elif (
                        "$lt" in self.transfer_rules["rules"][key]
                        and s[key] < self.transfer_rules["rules"][key]["$lt"]
                    ):
                        self.log("LT")

                        return True
                    else:
                        return False
        return False

    def buildUserMetadata(self, data, **kwargs):
        streamlist = []
        streamItem = {}
        streammeta = []
        settransfer = False
        streammeta = self.extractItemMetadata(data, kwargs["output_port"])
        if not isinstance(streammeta, list):
            streammeta = (
                kwargs["metadata"]
                if isinstance(kwargs["metadata"], list)
                else [kwargs["metadata"]]
            )
        elif isinstance(streammeta, list):
            try:
                if isinstance(kwargs["metadata"], list):
                    streammeta += kwargs["metadata"]
                if isinstance(kwargs["metadata"], dict):
                    for y in streammeta:
                        y.update(kwargs["metadata"])
            except Exception:
                traceback.print_exc(file=sys.stderr)
        if self.sel_rules is not None:
            self.provon = self.checkSelectiveRule(streammeta)

        if not self.provon:
            return streamItem
        streamItem.update(
            {
                "content": streammeta,
                "id": self.getUniqueId(data, kwargs["output_port"], **kwargs),
                "format": "",
                "location": "",
                "annotations": [],
                "port": kwargs["output_port"],
            },
        )
        streamItem.update(kwargs["control"])
        streamItem.update({"location": kwargs["location"], "format": kwargs["format"]})
        streamItem.update({"size": total_size(data)})

        if self.transfer_rules is not None:
            settransfer = self.checkTransferRule(streammeta)

        if settransfer:
            streamItem["s-prov:immediateAccess"] = True
            streamItem["s-prov:first-known-destination"] = self.transfer_rules[
                "destination"
            ]

        streamlist.append(streamItem)
        return streamlist

    def removeDerivation(self, **kwargs):
        if "name" in kwargs:
            id = self.getProvStateObjectId(kwargs["name"])
            for j in self.derivationIds:
                if j["DerivedFromDatasetID"] == id:
                    del self.derivationIds[self.derivationIds.index(j)]
        else:
            if "port" in kwargs:
                for j in self.derivationIds:
                    if j["port"] == kwargs["port"]:
                        del self.derivationIds[self.derivationIds.index(j)]

    def extractDataSourceId(self, data, port):
        """
        In support of the implementation of a _ProvenanceType_ realising a lineage _Pattern type_. Extract the id from the incoming data, if applicable,
        to reuse it to identify the correspondent provenance entity. This functionality is handy especially when a workflow component ingests data represented by
        self-contained and structured file formats. For instance, the NetCDF attributes Convention includes in its internal metadata an id that can be reused to ensure
        the linkage and therefore the consistent continuation of provenance tracesbetween workflow executions that generate and use the same data.
        """
        self.makeUniqueId(data, port)

    def buildDerivation(self, data, port=""):
        if data is not None and type(data) == dict and "id" in data:
            derivation = {
                "port": port,
                "DerivedFromDatasetID": data["id"],
                "TriggeredByProcessIterationID": data["TriggeredByProcessIterationID"],
                "prov_cluster": data["prov_cluster"],
                "iterationIndex": self.iterationIndex,
            }

            if port == "_d4p_state":
                derivation.update({"lookupterm": data["lookupterm"]})

            if "up:assertionType" in data:
                derivation.update({"up:assertionType": data["up:assertionType"]})

            self.derivationIds.append(derivation)

        else:
            id = self.extractDataSourceId(data, port)
            derivation = {
                "port": port,
                "DerivedFromDatasetID": id,
                "TriggeredByProcessIterationID": None,
                "prov_cluster": None,
                "iterationIndex": self.iterationIndex,
            }
            self.derivationIds.append(derivation)
            self.log("BUILDING INITIAL DERIVATION")

    def dicToKeyVal(self, dict, valueToString=False):
        try:
            alist = []
            for k, v in dict.iteritems():
                adic = {}
                adic.update({"key": str(k)})
                if valueToString:
                    adic.update({"val": str(v)})
                else:
                    try:
                        v = num(v)
                        adic.update({"val": v})
                    except Exception:
                        adic.update({"val": str(v)})

                alist.append(adic)

            return alist
        except Exception as err:
            self.error += f"{self.name} dicToKeyVal output Error: {err}"
            sys.stderr.write(f"ERROR: {self.name} dicToKeyVal output Error: {err}")
            traceback.print_exc(file=sys.stderr)


# Collection of Provenance Patterns Types


class AccumulateFlow(ProvenanceType):
    """
    A _Pattern type_ for a Processing Element (_s-prov:Component_) whose output depends on a sequence of input data; e.g. computation of periodic average.
    """

    def __init__(self):
        ProvenanceType.__init__(self)

    def apply_derivation_rule(
        self,
        event,
        voidInvocation,
        oport=None,
        iport=None,
        data=None,
        metadata=None,
    ):
        if event == "end_invocation_event" and voidInvocation is False:
            self.discardInFlow()


class Nby1Flow(ProvenanceType):
    """
    A _Pattern type_ for a Processing Element (_s-prov:Component_) whose output depends
    on the data received on all its input ports in lock-step; e.g. combined analysis of multiple
    variables.
    """

    def __init__(self):
        ProvenanceType.__init__(self)
        self.ports_lookups = {}

    def apply_derivation_rule(
        self,
        event,
        voidInvocation,
        oport=None,
        data=None,
        iport=None,
        metadata=None,
    ):
        for i in self.inputs:
            iport = i

        if event == "write":
            dep = []
            for x in self.inputconnections:
                if x != iport and x != "_d4py_feedback":
                    vv = self.ports_lookups[x].pop(0)
                    dep.append(vv)
                self.setStateDerivations(dep)

        if event == "end_invocation_event" and voidInvocation is True:
            if data is not None:
                # self.ports_lookups['iport'].append(vv)
                vv = str(abs(make_hash(tuple(iport + str(self.iterationIndex)))))
                if iport not in self.ports_lookups:
                    self.ports_lookups[iport] = []

                self.ports_lookups[iport].append(vv)
                self.update_prov_state(vv, None, metadata={"LOOKUP": str(vv)})
                self.discardInFlow()

        if event == "end_invocation_event" and voidInvocation is False:
            self.discardInFlow()
            self.discardState()


class SlideFlow(ProvenanceType):
    """
    A _Pattern type_ for a Processing Element (_s-prov:Component_) whose output depends
    on computations over sliding windows; e.g. computation of rolling sums.
    """

    def __init__(self):
        ProvenanceType.__init__(self)

    def apply_derivation_rule(
        self,
        event,
        voidInvocation,
        iport=None,
        oport=None,
        data=None,
        metadata=None,
    ):
        self.ignore_past_flow = False
        self.ignore_inputs = False
        self.stateful = False

        if event == "end_invocation_event":
            self.discardInFlow(wlength=self.WLENTGH)


class ASTGrouped(ProvenanceType):
    """
    A _Pattern type_ for a Processing Element (_s-prov:Component_) that manages a stateful operator
    with grouping rules; e.g. a component that produces a correlation matrix with the incoming
    coefficients associated with the same sampling-iteration index
    """

    def __init__(self):
        ProvenanceType.__init__(self)

    def apply_derivation_rule(
        self,
        event,
        voidInvocation,
        oport=None,
        iport=None,
        data=None,
        metadata=None,
    ):
        iport = None

        for i in self.inputs:
            iport = i

        # Do Before Writing
        if event == "write":
            vv = str(
                abs(
                    make_hash(
                        tuple(
                            [
                                self.getInputAt(port=iport, index=x)
                                for x in self.inputconnections[iport]["grouping"]
                            ],
                        ),
                    ),
                ),
            )
            self.log(f"LOOKUP: {vv}")
            self.setStateDerivations([vv])

        if event == "end_invocation_event" and voidInvocation is False:
            self.discardInFlow()
            self.discardState()

        if event == "end_invocation_event" and voidInvocation is True:
            if data is not None:
                vv = str(
                    abs(
                        make_hash(
                            tuple(
                                [
                                    self.getInputAt(port=iport, index=x)
                                    for x in self.inputconnections[iport]["grouping"]
                                ],
                            ),
                        ),
                    ),
                )
                self.ignorePastFlow()
                self.update_prov_state(vv, data, metadata={"LOOKUP": str(vv)}, dep=[vv])
                self.discardInFlow()
                self.discardState()


class SingleInvocationFlow(ProvenanceType):
    """
    A _Pattern type_ for a Processing Element (_s-prov:Component_) that
    presents stateless input output dependencies; e.g. the Processing Element of a simple I/O
    pipeline.
    """

    def __init__(self):
        ProvenanceType.__init__(self)

    def apply_derivation_rule(
        self,
        event,
        voidInvocation,
        iport=None,
        oport=None,
        data=None,
        metadata=None,
    ):
        if (event == "end_invocation_event") and voidInvocation:
            self.discardInFlow(discardState=True)

        if (event == "end_invocation_event") and not voidInvocation:
            self.discardInFlow(discardState=True)


class AccumulateStateTrace(ProvenanceType):
    """
    A _Pattern type_ for a Processing Element (_s-prov:Component_) that
    keeps track of the updates on intermediate results written to the output after a sequence
    of inputs; e.g. traceable approximation of frequency counts or of periodic averages.
    """

    def __init__(self):
        ProvenanceType.__init__(self)

    def apply_derivation_rule(
        self,
        event,
        voidInvocation,
        port=None,
        data=None,
        metadata=None,
    ):
        self.ignore_past_flow = False
        self.ignore_inputs = False
        if event == "write" and port == self.STATEFUL_PORT:
            self.update_prov_state(self.STATEFUL_PORT, data, metadata=metadata)
        if event == "write" and port != self.STATEFUL_PORT:
            self.ignorePastFlow()
        if event == "end_invocation_event" and voidInvocation is False:
            self.discardInFlow()
            self.discardState()


class IntermediateStatefulOut(ProvenanceType):
    """
    A _Pattern type_ for a Processing Element (_s-prov:Component_) stateful component which produces distinct but interdependent
    output; e.g. detection of events over periodic observations or any component that reuses the data just written to generate a new product
    """

    def __init__(self):
        ProvenanceType.__init__(self)

    def apply_derivation_rule(
        self,
        event,
        voidInvocation,
        iport=None,
        oport=None,
        data=None,
        metadata=None,
    ):
        self.ignore_past_flow = False
        self.ignore_inputs = False
        self.stateful = False
        if event == "write" and oport == self.STATEFUL_PORT:
            self.update_prov_state(self.STATEFUL_PORT, data, metadata=metadata)

        if event == "write" and oport != self.STATEFUL_PORT:
            self.ignorePastFlow()

        if event == "end_invocation_event" and voidInvocation is False:
            self.discardInFlow()
            self.discardState()


class ForceStateless(ProvenanceType):
    """
    A _Pattern type_ for a Processing Element (_s-prov:Component_). It considers the outputs of the component dependent
    only on the current input data, regardless from any explicit state update; e.g. the user wants to reduce the
    amount of lineage produced by a component that presents inline calls to the _update_prov_state_, accepting less accuracy.
    """

    def __init__(self):
        ProvenanceType.__init__(self)
        self.streammeta = []
        self.count = 1

    def apply_derivation_rule(self, event, voidInvocation):
        if event == "state":
            self.provon = False

        super(ProvenanceOnWriteOnly, self).apply_derivation_rule(event, voidInvocation)


meta = True


def get_source(object, spacing=10, collapse=1):
    """
    Print methods and doc strings.
    Takes module, class, list, dictionary, or string.
    """

    methodList = [e for e in dir(object) if callable(getattr(object, e))]
    processFunc = collapse and (lambda s: " ".join(s.split())) or (lambda s: s)
    return "\n".join(
        [
            "{} {}".format(
                (
                    method.ljust(spacing),
                    processFunc(str(getattr(object, method).__doc__)),
                )
                for method in methodList
            ),
        ],
    )


namespaces = {}


def injectProv(
    object,
    provType,
    active=True,
    componentsType=None,
    workflow=None,
    **kwargs,
):
    """
    This function dinamically extend the type of each the nodes of the graph
    or subgraph with ProvenanceType type or its specialisation
    """

    if workflow is None:
        workflow = {}
    if isinstance(object, WorkflowGraph):
        object.flatten()
        workflow = {}
        nodelist = object.get_contained_objects()

        for x in nodelist:
            injectProv(
                x,
                provType,
                componentsType=componentsType,
                workflow=workflow,
                **kwargs,
            )
    else:
        print(
            f"Assigning Provenance Type to: \r{object.name} Original base class: {object.__class__.__bases__}",
        )
        parent = object.__class__.__bases__[0]
        localname = object.name

        if componentsType is not None and object.name in componentsType:
            body = {}

            if "s-prov:type" in componentsType[object.name]:
                for x in componentsType[object.name]["s-prov:type"]:
                    body.update(x().__dict__)
                object.__class__ = type(
                    str(object.__class__),
                    componentsType[object.name]["s-prov:type"] + (object.__class__,),
                    body,
                )

            else:
                body = {}
                for x in provType:
                    body.update(x().__dict__)
                object.__class__ = type(
                    str(object.__class__),
                    (*provType, object.__class__),
                    body,
                )

            # if any associates a statful port to the provenance type
            if "s-prov:stateful-port" in componentsType[object.name]:
                object.STATEFUL_PORT = componentsType[object.name][
                    "s-prov:stateful-port"
                ]
            if "wlength" in componentsType[object.name]:
                object.WLENTGH = componentsType[object.name]["wlength"]
            if "s-prov:prov-cluster" in componentsType[object.name]:
                object.prov_cluster = componentsType[object.name]["s-prov:prov-cluster"]

        else:
            body = {}
            print(provType)
            for x in provType:
                body.update(x().__dict__)
            object.__class__ = type(
                str(object.__class__),
                (*provType, object.__class__),
                body,
            )

        object.comp_id = object.id
        object.pe_init(pe_class=parent, **kwargs)

        print(f" New type: {object.__class__.__bases__}\r")
        object.name = localname

        code = ""
        import pprint

        # check if the PE is defined as a SimpleFunction and capture its defining function
        # if isinstance(object, (SimpleFunctionPE)):
        for x in inspect.getmembers(object, predicate=inspect.isfunction):
            code += pprint.pformat(inspect.getsource(x[1]), indent=1, compact=False)

        if len(code) == 0:
            for x in inspect.getmembers(object, predicate=inspect.ismethod):
                if x[1].__name__ == "_process":
                    code += pprint.pformat(
                        inspect.getsource(x[1]),
                        indent=1,
                        compact=False,
                    )

        workflow.update(
            {
                object.id: {
                    "type": str(object.__class__.__bases__),
                    "code": code,
                    "functionName": object.name,
                },
            },
        )
        if hasattr(object, "ns"):
            namespaces.update(object.ns)
    return workflow


provclusters = {}

prov_save_mode = {}


def update_prov_run(runId, save_mode="file", dic=None):
    d4py_udpaterun = UpdateWorkflowRun(save_mode)
    if dic is not None:
        d4py_udpaterun.parameters = dic
        d4py_udpaterun.parameters.update({"runId": runId})
    _graph = WorkflowGraph()
    provrec = None
    provrec = IterativePE()
    _graph.connect(d4py_udpaterun, "output", provrec, "input")

    simple_process.process(_graph, {"UpdateWorkflowRun": [{"input": "None"}]})
    _graph = None


def load_provenance_config(configfile):
    with open(configfile) as cfg_file:
        return json.load(cfg_file)


def create_provenance_argparser():
    parser = argparse.ArgumentParser(description="Submit provenance parameters.")
    parser.add_argument(
        "--provenance-repository-url",
        dest="prov_repo_url",
        nargs="?",
        required=False,
        help=(
            "Indicates the repository endpoint (S-ProvFlow) where the provenance will be sent "
            "if save-mode in provenance config is 'service', in which case this argument is "
            "mandatory."
        ),
    )
    parser.add_argument(
        "--provenance-export-url",
        dest="prov_export_url",
        nargs="?",
        required=False,
        help=(
            "The service endpoint from where the provenance of a workflow execution,"
            "after being stored, can be extracted in PROV format."
        ),
    )
    parser.add_argument(
        "--provenance-path",
        dest="prov_path",
        nargs="?",
        required=False,
        help=(
            "indicates a file system path where the lineage will be stored. Mandatory "
            "if save-mode in provenance configuration is 'file'."
        ),
    )
    parser.add_argument(
        "--provenance-bulk-size",
        dest="prov_bulk_size",
        nargs="?",
        required=False,
        type=int,
        help=(
            "Number of lineage documents to be stored in a single file or in a single"
            "request to the remote service. Helps tuning the overhead brought by the latency"
            "of accessing storage resources."
        ),
    )
    parser.add_argument(
        "--provenance-runid",
        dest="prov_runid",
        nargs="?",
        required=False,
        help=(
            "Run ID of the run. This is mandatory if the target is 'mpi' "
            "and there is no run-id in the provenance configuration."
        ),
    )
    parser.add_argument(
        "--provenance-userid",
        dest="prov_userid",
        nargs="?",
        required=False,
        help=("User ID the user will be identified with in the provenance documents."),
    )
    parser.add_argument(
        "--provenance-bearer-token",
        dest="prov_bearer_token",
        nargs="?",
        required=False,
        help=("Token that will be used to authenticate agains the sprov-api"),
    )
    return parser


def components_type_str_list_2_class_tuple(prov_config):
    ## Figure out the module name of the graph and import it;
    ## The component types must be resolved through its imports.
    from importlib import import_module

    module_name = os.path.splitext(os.path.basename(CommandLineInputs.module))[0]
    module = import_module(module_name)

    if "s-prov:componentsType" in prov_config:
        for prov_ct_name in prov_config["s-prov:componentsType"]:
            prov_ct = prov_config["s-prov:componentsType"][prov_ct_name]
            component_type_list = []
            if isinstance(
                prov_ct["s-prov:type"],
                list,
            ):  # Inline config may contain list of classnames or tuple with clases
                # Obtain the list of component types and convert the strings
                # to python classes and add them to the s-prov:type list as
                # classes, not as the strings they are in the json file.
                for ct in prov_ct["s-prov:type"]:
                    component_type = module.__dict__[ct]
                    component_type_list.append(component_type)
                # Convert to tuple, because injectProv() appends it with tuple
                # and you cannot append a tuple to a list.
                prov_ct["s-prov:type"] = tuple(component_type_list)


def init_provenance_config(args):
    provparser = create_provenance_argparser()
    provenance_args, remaining = provparser.parse_known_args()

    # init_provenance_config is also called when inline provenance config is used.
    # Overwrite class variables only if present in commandline
    if provenance_args.prov_repo_url:
        ProvenanceType.REPOS_URL = provenance_args.prov_repo_url
    if provenance_args.prov_export_url:
        ProvenanceType.PROV_EXPORT_URL = provenance_args.prov_export_url
    if provenance_args.prov_bearer_token:
        ProvenanceType.PROV_BEARER_TOKEN = provenance_args.prov_bearer_token
    if provenance_args.prov_path:
        ProvenanceType.PROV_PATH = provenance_args.prov_path
    if provenance_args.prov_bulk_size:
        ProvenanceType.BULK_SIZE = provenance_args.prov_bulk_size

    if args.provenance and args.provenance != "inline":
        prov_config = load_provenance_config(args.provenance)
    elif CommandLineInputs.inline_prov_config:
        prov_config = CommandLineInputs.inline_prov_config
    else:
        print(
            "\nWARNING: User should supply either inline provenance config in dispel4py workflow or specify\n"
            "a file for the --provenance-config argument.\n Processing continuing without provenance recordings",
        )
        return False, False

    prov_config["s-prov:mapping"] = args.target

    if provenance_args.prov_runid:
        prov_config["s-prov:run-id"] = provenance_args.prov_runid
    if provenance_args.prov_userid:
        prov_config["provone:User"] = provenance_args.prov_userid

    if "provone:User" not in prov_config:
        print(
            "\nWARNING: No username is supplied, neither inline or via the command line. Assuming user anonymous",
        )
        prov_config["provone:User"] = "anonymous"

    if "s-prov:save-mode" in prov_config:
        if prov_config["s-prov:save-mode"] == "file" and not provenance_args.prov_path:
            print(
                "\ns-prov:save-mode is 'file', but no --provenance-path argument supplied!\n",
            )
            provparser.print_help()
            sys.exit(1)
        if (
            prov_config["s-prov:save-mode"] == "service"
            and not provenance_args.prov_repo_url
        ):
            print(
                "\ns-prov:save-mode is 'service', but no --provenance-repository-url argument supplied!\n",
            )
            provparser.print_help()
            sys.exit(1)
    else:
        if provenance_args.prov_repo_url:
            prov_config["s-prov:save-mode"] = "service"
        elif provenance_args.prov_path:
            prov_config["s-prov:save-mode"] == "file"
        else:
            print(
                "\nMust supply either --provenance-repository-url or --provenance-path argument.\n",
            )
            provparser.print_help()
            sys.exit(1)

    ## Also return remaining in case any one is ever interested.
    return prov_config, remaining


class CommandLineInputs:
    """
    Holds the "global" variables.
    - inputs: Inputs as specified on command line as -d
    - provenanceCommandLineConfigPresent: Is set when procenance config is specified using --provenance-config
    """

    inputs = {}
    provenanceCommandLineConfigPresent = False
    inline_prov_config = {}
    inline_graph = None
    module = None


def configure_prov_run(
    graph,
    provRecorderClass=None,
    provImpClass=ProvenanceType,
    input=None,
    username=None,
    workflowId=None,
    description=None,
    system_id=None,
    workflowName=None,
    workflowType=None,
    w3c_prov=False,
    runId=None,
    componentsType=None,
    clustersRecorders=None,
    feedbackPEs=None,
    save_mode="file",
    sel_rules=None,
    transfer_rules=None,
    update=False,
    sprovConfig=None,
    sessionId=None,
    mapping="simple",
    force=False,  # For internal use: force execution even if provenanceConfig is set.
    #    This ensures the command line provenance configuration has got higher priority over the inline configuration
):
    """
    In order to enable the user of a data-intensive application to configure the lineage metadata extracted from the execution of their
    worklfows we adopt a provenance configuration profile. The configuration is used at the time of the initialisation of the workflow to prepare its provenance-aware
    execution. We consider that a chosen configuration may be influenced by personal and community preferences, as well as by rules introduced by institutional policies.
    For instance, a certain RI would require to choose among a set of contextualisation types, in order to adhere to
    the infrastructure's metadata portfolio. Thus, a provenance configuration profile play
    in favour of more generality, encouraging the implementation and the re-use of fundamental
    methods across disciplines.

    With this method, the users of the workflow provide general provenance information on the attribution of the run, such as _username_, _runId_ (execution id),
    _description_, _workflowName_, and its semantic characterisation _workflowType_. It allows users to indicate which provenance types to apply to each component
    and the belonging conceptual provenance cluster. Moreover, users can also choose where to store the lineage (_save_mode_), locally in the file system or in a remote service or database.
    Lineage storage operations can be performed in bulk, with different impacts on the overall overhead and on the experienced rapidity of access to the lineage information.

    - __Configuration JSON__: We show here an example of the JSON document used to prepare a worklfow for a provenance aware execution. Some properties are described inline. These are defined by terms in the provone and s-prov namespaces.

    ```python
        {
                'provone:User': "aspinuso",
                's-prov:description' : "provdemo demokritos",
                's-prov:workflowName': "demo_epos",
                # Assign a generic characterisation or aim of the workflow
                's-prov:workflowType': "seis:preprocess",
                # Specify the unique id of the workflow
                's-prov:workflowId'  : "workflow process",
                # Specify whether the lineage is saved locally to the file system or remotely to an existing serivce (for location setup check the class prperties or the command line instructions section.)
                's-prov:save-mode'   : 'service'         ,
                # Assign the Provenance Types and Provenance Clusters to the processing elements of the workflows. These are indicated by the name attributed to their class or function, eg. PE_taper. The 's-prov:type' property accepts a list of class names, corrisponding to the types' implementation. The 's-prov:cluster' is used to group more processing elements to a common functional section of the workflow.
                's-prov:componentsType' :
                                   {'PE_taper': {'s-prov:type':["SeismoPE"]),
                                                 's-prov:prov-cluster':'seis:Processor'},
                                    'PE_plot_stream':    {'s-prov:prov-cluster':'seis:Visualisation',
                                                       's-prov:type':["SeismoPE"]},
                                    'StoreStream':    {'s-prov:prov-cluster':'seis:DataHandler',
                                                       's-prov:type':["SeismoPE,AccumulateFlow"]}
                                    }}
    ```

    - __Selectivity rules__: By declaratively indicating a set of Selectivity rules for every component ('s-prov:sel_rules'), users can respectively activate the collection
    of the provenance for particular Data elements or trigger transfer operations of the data to external locations. The approach takes advantage of the contextualisation
    possibilities offered by the provenance _Contextualisation types_. The rules consist of comparison expressions formulated in JSON that indicate the boundary
    values for a specific metadata term. Such representation is inspired by the query language and selectors adopted by a popular document store, MongoDB.
    These can be defined also within the configuration JSON introduced above.

    Example, a Processing Element _CorrCoef_ that produces lineage information only when the _rho_ value is greater than 0:
    ```python
        { "CorrCoef": {
            "rules": {
                "rho": {
                    "$gt": 0
        }}}}
    ```

    - __Command Line Activation__: To enable proveance activation through command line dispel4py should be executed with specific command line instructions. The following command will execute a local test for the provenance-aware execution of the MySplitAndMerge workflow.

    ```python
    dispel4py --provenance-config=dispel4py/examples/prov_testing/prov-config-mysplitmerge.json --provenance-repository-url=http://testbed.project-dare.eu/prov/workflowexecutions/insert multi dispel4py/examples/prov_testing/mySplitMerge_prov.py -n 10
    ```

    """

    # When a configuration is set using command line argument "provenance-config",
    # configure_prov_run should only run when force argument is set to True.
    # When e.g. called from workflow script and the provenance-config argument
    # is present, the force argument defaults to False and the inline provenance
    # configuration in the workflow is ignored.
    if transfer_rules is None:
        transfer_rules = {}
    if sel_rules is None:
        sel_rules = {}
    if feedbackPEs is None:
        feedbackPEs = []
    if clustersRecorders is None:
        clustersRecorders = {}
    if input is None:
        input = []
    if CommandLineInputs.provenanceCommandLineConfigPresent and not force:
        if graph:
            CommandLineInputs.inline_graph = graph
        if sprovConfig:
            CommandLineInputs.inline_prov_config = sprovConfig
        if runId:
            CommandLineInputs.inline_prov_config["s-prov:run-id"] = runId
        print(
            "Provenance configuration specified inline from workflow module, but command line\n"
            "configuration implied. Inline module provenance configuration saved, but might be\n"
            "overridden by configuration specified on command line.",
        )
        return None

    if sprovConfig:
        components_type_str_list_2_class_tuple(sprovConfig)
        if "s-prov:run-id" in sprovConfig:
            runId = sprovConfig["s-prov:run-id"]
        if "s-prov:session-id" in sprovConfig:
            sessionId = sprovConfig["s-prov:session-id"]
        if "s-prov:system-id" in sprovConfig:
            system_id = sprovConfig["s-prov:system-id"]
        if "s-prov:transfer-rules" in sprovConfig:
            transfer_rules = sprovConfig["s-prov:transfer-rules"]
        if "s-prov:WFExecutionInputs" in sprovConfig:
            input = sprovConfig["s-prov:WFExecutionInputs"]
        if "provone:User" in sprovConfig:
            username = sprovConfig["provone:User"]
        if "s-prov:workflowId" in sprovConfig:
            workflowId = sprovConfig["s-prov:workflowId"]
        if "s-prov:description" in sprovConfig:
            description = sprovConfig["s-prov:description"]
        if "s-prov:workflowName" in sprovConfig:
            workflowName = sprovConfig["s-prov:workflowName"]
        if "s-prov:sel-rules" in sprovConfig:
            sel_rules = sprovConfig["s-prov:sel-rules"]
        if "s-prov:workflowType" in sprovConfig:
            workflowType = sprovConfig["s-prov:workflowType"]
        if "s-prov:componentsType" in sprovConfig:
            componentsType = sprovConfig["s-prov:componentsType"]
        if "s-prov:save-mode" in sprovConfig:
            save_mode = sprovConfig["s-prov:save-mode"]
        if "s-prov:mapping" in sprovConfig:
            mapping = sprovConfig["s-prov:mapping"]

    if not update and (username is None or workflowId is None or workflowName is None):
        raise Exception("Missing values")
    if runId is None and mapping != "mpi":
        runId = getUniqueId()
    elif runId is None and mapping == "mpi":
        print(
            "\n Auto-generation of runId not supported for MPI targets.\n",
            "Provide a value for the 's-prov:run-id' key in the provenance configuration\n",
            "and run again!\n",
        )
        sys.exit(1)
    if not sessionId and "SPROV_SESSIONID" in os.environ:
        sessionId = os.environ["SPROV_SESSIONID"]

    if CommandLineInputs.inputs:
        # If inputs are given in the command line, using -d or -f, these inputs should be added
        # to the inputs defined as WFExecutionInputs in the workflow configuration.
        cl_input = SPROV_CL_D4PY_INPUT
        cl_input["value"] = json.dumps(CommandLineInputs.inputs).encode()

        input.append(cl_input)

    print("Change grouping implementation ")

    dispel4py.new.processor.GroupByCommunication.get_destination = getDestination_prov
    global meta

    workflow = injectProv(
        graph,
        provImpClass,
        componentsType=componentsType,
        save_mode=save_mode,
        controlParameters={"username": username, "runId": runId},
        sel_rules=sel_rules,
        transfer_rules=transfer_rules,
    )

    d4py_newrun = NewWorkflowRun(save_mode)

    d4py_newrun.parameters = {
        "input": input,
        "username": username,
        "workflowId": workflowId,
        "description": description,
        "system_id": system_id,
        "workflowName": workflowName,
        "runId": runId,
        "sessionId": sessionId,
        "mapping": mapping,
        "sel_rules": sel_rules,
        "transfer_rules": transfer_rules,
        "source": workflow,
        "ns": namespaces,
        "workflowType": workflowType,
        "update": update,
        "status": "active",
    }

    mpirank = 0
    if mapping == "mpi" and mapping in dispel4py.new.mappings.config:
        # Determine the rank
        from importlib import import_module

        mpimodule = import_module(dispel4py.new.mappings.config[mapping], mapping)
        mpirank = mpimodule.rank

    if mpirank == 0:
        # newrun.parameters=clean_empty(newrun.parameters)
        _graph = WorkflowGraph()
        provrec = None

        if provRecorderClass is not None:
            provrec = provRecorderClass(toW3C=w3c_prov)
            _graph.connect(d4py_newrun, "output", provrec, "metadata")
        else:
            provrec = IterativePE()
            _graph.connect(d4py_newrun, "output", provrec, "input")

        simple_process.process(_graph, {"NewWorkflowRun": [{"input": "None"}]})

    if provRecorderClass is not None:
        print("PREPARING PROVENANCE SENSORS:")
        print("Provenance Recorders Clusters: " + str(clustersRecorders))
        print("PEs processing Recorders feedback: " + str(feedbackPEs))

        ProvenanceType.send_prov_to_sensor = True
        attachProvenanceRecorderPE(
            graph,
            provRecorderClass,
            runId,
            username,
            w3c_prov,
            clustersRecorders,
            feedbackPEs,
        )

    return runId


def attachProvenanceRecorderPE(
    graph,
    provRecorderClass,
    runId=None,
    username=None,
    w3c_prov=False,
    clustersRecorders=None,
    feedbackPEs=None,
):
    if feedbackPEs is None:
        feedbackPEs = []
    if clustersRecorders is None:
        clustersRecorders = {}
    provclusters = {}
    partitions = []
    provtag = None
    try:
        partitions = graph.partitions
    except:
        print(f"NO PARTITIONS: {partitions}")

    if username is None or runId is None:
        raise Exception("Missing values")
    graph.flatten()

    nodelist = graph.get_contained_objects()

    recpartition = []
    for x in nodelist:
        if isinstance(x, (WorkflowGraph)):
            attachProvenanceRecorderPE(
                x,
                provRecorderClass,
                runId=runId,
                username=username,
                w3c_prov=w3c_prov,
            )

        if isinstance(x, (ProvenanceType)) and x.provon:
            provrecorder = provRecorderClass(toW3C=w3c_prov)
            if isinstance(x, (SimpleFunctionPE)):
                if "prov_cluster" in x.params:
                    provtag = x.params["prov_cluster"]
                    x.prov_cluster = provtag

            else:
                if hasattr(x, "prov_cluster"):
                    provtag = x.prov_cluster

            if provtag is not None:
                "checks if specific recorders have been indicated"
                if provtag in clustersRecorders:
                    provrecorder = clustersRecorders[provtag](toW3C=w3c_prov)

                print(
                    f"PROV CLUSTER: Attaching {x.name} to provenance cluster: {provtag} with recorder: {provrecorder}",
                )

                if provtag not in provclusters:
                    provclusters[provtag] = provrecorder
                else:
                    provrecorder = provclusters[provtag]

            x.controlParameters["runId"] = runId
            x.controlParameters["username"] = username
            provport = str(id(x))
            provrecorder._add_input(provport, grouping=["prov_cluster"])
            provrecorder._add_output(provport)
            provrecorder.porttopemap[x.name] = provport

            graph.connect(x, OUTPUT_METADATA, provrecorder, provport)
            if x.name in feedbackPEs:
                y = PassThroughPE()
                graph.connect(provrecorder, provport, y, "input")
                graph.connect(y, "output", x, "_d4py_feedback")

            recpartition.append(provrecorder)
            partitions.append([x])
            provtag = None

    return graph


class ProvenanceSimpleFunctionPE(ProvenanceType):
    """A _Pattern type_ for the native  _SimpleFunctionPE_ of dispel4py"""

    def __init__(self, *args, **kwargs):
        self.__class__ = type(
            str(self.__class__),
            (self.__class__, SimpleFunctionPE),
            {},
        )
        SimpleFunctionPE.__init__(self, *args, **kwargs)
        ProvenanceType.__init__(self, *args, **kwargs)


class ProvenanceIterativePE(ProvenanceType):
    """A _Pattern type_ for the native  _IterativePE_ Element of dispel4py"""

    def __init__(self, *args, **kwargs):
        self.__class__ = type(str(self.__class__), (self.__class__, IterativePE), {})
        IterativePE.__init__(self, *args, **kwargs)
        ProvenanceType.__init__(self, *args, **kwargs)


class NewWorkflowRun(ProvenanceType):
    def __init__(self, save_mode):
        ProvenanceType.__init__(self)
        self.pe_init(pe_class=ProvenanceType, save_mode=save_mode)
        self._add_output("output")

    def packageAll(self, contentmeta):
        return {"metadata": contentmeta[0]["content"][0]}

    def makeRunMetadataBundle(
        self,
        input=None,
        username=None,
        workflowId=None,
        description="",
        system_id=None,
        workflowName=None,
        workflowType=None,
        w3c=False,
        runId=None,
        sessionId=None,
        modules=None,
        subProcesses=None,
        ns=None,
        update=False,
        status=None,
    ):
        if input is None:
            input = []
        bundle = {}
        if not update and (
            username is None or workflowId is None or workflowName is None
        ):
            raise Exception("Missing values")
        else:
            if runId is None:
                bundle["_id"] = getUniqueId()
            else:
                bundle["_id"] = runId

            bundle["runId"] = bundle["_id"]
            bundle["sessionId"] = sessionId
            bundle["input"] = input
            bundle["startTime"] = str(datetime.datetime.utcnow())
            bundle["username"] = username
            bundle["workflowId"] = workflowId
            bundle["description"] = description
            bundle["system_id"] = system_id
            bundle["workflowName"] = workflowName
            bundle["mapping"] = self.parameters["mapping"]
            bundle["type"] = "workflow_run"
            bundle["modules"] = modules
            bundle["prov:type"] = workflowType
            bundle["source"] = subProcesses
            bundle["ns"] = ns
            bundle["status"] = status
            bundle = clean_empty(bundle)

        return bundle

    def _process(self, inputs):
        self.name = "NewWorkflowRun"

        bundle = self.makeRunMetadataBundle(
            username=self.parameters["username"],
            input=self.parameters["input"],
            workflowId=self.parameters["workflowId"],
            description=self.parameters["description"],
            system_id=self.parameters["system_id"],
            workflowName=self.parameters["workflowName"],
            workflowType=self.parameters["workflowType"],
            runId=self.parameters["runId"],
            sessionId=self.parameters["sessionId"],
            modules=sorted(
                [f"{i.key}=={i.version}" for i in get_installed_distributions()],
            ),
            subProcesses=self.parameters["source"],
            ns=self.parameters["ns"],
            status=self.parameters["status"],
        )

        self.log("STORING WORKFLOW RUN METADATA")

        self.write("output", bundle, metadata=bundle)


class UpdateWorkflowRun(ProvenanceType):
    def __init__(self, save_mode):
        ProvenanceType.__init__(self)
        self.pe_init(pe_class=ProvenanceType, save_mode=save_mode)
        self._add_output("output")

    def packageAll(self, contentmeta):
        return {"metadata": contentmeta[0]["content"][0]}

    def _process(self, inputs):
        self.name = "UpdateWorkflowRun"
        self.log(self.parameters)
        self.parameters.update({"type": "workflow_run"})
        self.log(self.parameters)
        self.log(f"UPDATING WORKFLOW RUN METADATA{self.parameters}")

        self.write("output", self.parameters, metadata=self.parameters)


class PassThroughPE(IterativePE):
    def _process(self, data):
        self.write("output", data)


class ProvenanceRecorder(GenericPE):
    INPUT_NAME = "metadata"
    REPOS_URL = ""

    def __init__(self, name="ProvenanceRecorder", toW3C=False):
        GenericPE.__init__(self)
        self.porttopemap = {}
        self._add_output("feedback")
        self._add_input(ProvenanceRecorder.INPUT_NAME)


class ProvenanceRecorderToFile(ProvenanceRecorder):
    def __init__(self, name="ProvenanceRecorderToFile", toW3C=False):
        ProvenanceRecorder.__init__(self)
        self.name = name
        self.convertToW3C = toW3C

    def process(self, inputs):
        for x in inputs:
            prov = inputs[x]
        out = None

        if isinstance(prov, list) and "data" in prov[0]:
            prov = prov[0]["data"]

        out = toW3Cprov(prov) if self.convertToW3C else prov

        filep = open(f"{os.environ['PROV_PATH']}/{prov['_id']}", "wr")
        json.dump(out, filep)
        filep.close()


class ProvenanceRecorderToService(ProvenanceRecorder):
    def __init__(self, name="ProvenanceRecorderToService", toW3C=False):
        ProvenanceRecorder.__init__(self)
        self.name = name
        self.convertToW3C = toW3C

    def _preprocess(self):
        self.provurl = urlparse(ProvenanceRecorder.REPOS_URL)
        self.connection = HTTPConnection(self.provurl.netloc)

    def _process(self, inputs):
        # ports are assigned automatically as numbers, we just need to read from any of these
        for x in inputs:
            prov = inputs[x]

        out = None
        if isinstance(prov, list) and "data" in prov[0]:
            prov = prov[0]["data"]

        out = toW3Cprov(prov) if self.convertToW3C else prov

        params = urlencode({"prov": json.dumps(out)})
        headers = {
            "Content-type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        }
        self.connection.request("POST", self.provurl.path, params, headers)

        self.connection.getresponse()
        self.connection.close()

    def postprocess(self):
        self.connection.close()


class ProvenanceRecorderToServiceBulk(ProvenanceRecorder):
    def __init__(self, name="ProvenanceRecorderToServiceBulk", toW3C=False):
        ProvenanceRecorder.__init__(self)
        self.name = name
        self.convertToW3C = toW3C
        self.bulk = []
        self.numprocesses = 2
        self.timestamp = datetime.datetime.utcnow()

    def _preprocess(self):
        self.provurl = urlparse(ProvenanceRecorder.REPOS_URL)

        self.connection = HTTPConnection(self.provurl.netloc)

    def postprocess(self):
        if len(self.bulk) > 0:
            params = urlencode({"prov": json.dumps(self.bulk)})
            headers = {
                "Content-type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            }
            self.connection.request("POST", self.provurl.path, params, headers)
            self.connection.getresponse()
            self.connection.close()

    def _process(self, inputs):
        prov = None
        for x in inputs:
            prov = inputs[x]
        out = None

        if isinstance(prov, list) and "_d4p" in prov[0]:
            prov = prov[0]["_d4p"]
        elif "_d4p" in prov:
            prov = prov["_d4p"]

        out = toW3Cprov(prov) if self.convertToW3C else prov

        self.bulk.append(out)

        if len(self.bulk) == 100:
            # self.log("TO SERVICE ________________ID: "+str(self.bulk))
            params = urlencode({"prov": json.dumps(self.bulk)})
            headers = {
                "Content-type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            }
            self.connection.request("POST", self.provurl.path, params, headers)
            self.connection.getresponse()
            self.connection.close()
            self.bulk[:] = []


# For dynamic re-implementation testing purposes


def new_process(self, data):
    self.log("I AM NEW FROM RECORDER")
    self.operands.append(data["input"])
    if len(self.operands) == 2:
        val = (self.operands[0] - 1) / self.operands[1]
        self.write("output", val, metadata={"new_val": val})
        self.log(f"New Imp from REC !!!! {val}")
        self.operands = []


# Test Recoder Class providing new implementation all
# The instances of an attached PE


class ProvenanceRecorderToFileBulk(ProvenanceRecorder):
    def __init__(self, name="ProvenanceRecorderToFileBulk", toW3C=False):
        ProvenanceRecorder.__init__(self)
        self.name = name
        self.convertToW3C = toW3C
        self.bulk = []

    def postprocess(self):
        filep = open(f"{os.environ['PROV_PATH']}/bulk_{getUniqueId()}", "wr")
        json.dump(self.bulk, filep)
        self.bulk[:] = []

    def process(self, inputs):
        out = None
        for x in inputs:
            prov = inputs[x]

        if isinstance(prov, list) and "data" in prov[0]:
            prov = prov[0]["data"]
        elif "_d4p" in prov:
            prov = prov["_d4p"]

        out = toW3Cprov(prov) if self.convertToW3C else prov

        self.bulk.append(out)
        if len(self.bulk) == 140:
            filep = open(f"{os.environ['PROV_PATH']}/bulk_{getUniqueId()}", "wr")
            json.dump(self.bulk, filep)
            self.bulk[:] = []


class MyProvenanceRecorderWithFeedback(ProvenanceRecorder):
    def __init__(self, toW3C=False):
        ProvenanceRecorder.__init__(self)
        self.convertToW3C = toW3C
        self.bulk = []
        self.timestamp = datetime.datetime.utcnow()

    def _preprocess(self):
        self.provurl = urlparse(ProvenanceRecorder.REPOS_URL)

        self.connection = HTTPConnection(self.provurl.netloc)

    def postprocess(self):
        self.connection.close()

    def _process(self, inputs):
        prov = None
        for x in inputs:
            prov = inputs[x]
        out = None
        if isinstance(prov, list) and "data" in prov[0]:
            prov = prov[0]["data"]

        out = toW3Cprov(prov) if self.convertToW3C else prov

        self.write(self.porttopemap[prov["name"]], "FEEDBACK MESSAGGE FROM RECORDER")

        self.bulk.append(out)
        params = urlencode({"prov": json.dumps(self.bulk)})
        headers = {
            "Content-type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        }
        self.connection.request("POST", self.provurl.path, params, headers)
        self.connection.getresponse()
