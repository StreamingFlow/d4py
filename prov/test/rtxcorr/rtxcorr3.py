import os
import random
import time
import traceback
import urllib

import httplib
import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
import pandas as pd
import seaborn as sns
import ujson
from dateutil.parser import parse as parse_date

from dispel4py.base import (
    GenericPE,
)
from dispel4py.new.processor import *
from dispel4py.provenance import *
from dispel4py.workflow_graph import WorkflowGraph

sns.set(style="white")


class Start(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input("iterations")
        self._add_output("output")
        # self.prov_cluster="myne"

    def _process(self, inputs):
        if "iterations" in inputs:
            inp = inputs["iterations"]

            self.write("output", inp, metadata={"iterations": inp})

        # Uncomment this line to associate this PE to the mycluster provenance-cluster
        # self.prov_cluster ='mycluster'


class Source(GenericPE):
    def __init__(self, sr, index):
        GenericPE.__init__(self)
        self._add_input("iterations")
        self._add_output("output")
        self.sr = sr
        self.var_index = index
        # self.prov_cluster="myne"

        self.parameters = {"sampling_rate": sr}

        # Uncomment this line to associate this PE to the mycluster provenance-cluster
        # self.prov_cluster ='mycluster'

    def _process(self, inputs):
        if "iterations" in inputs:
            iteration = inputs["iterations"][0]

        # Streams out values at 1/self.sr sampling rate, until iteration>0
        while iteration > 0:
            val = random.uniform(0, 100)
            time.sleep(1 / self.sr)
            iteration -= 1
            self.write(
                "output",
                (self.name, val),
                metadata={
                    "val": val,
                    "var_index": self.var_index,
                    "iteration": iteration,
                },
            )


class MaxClique(GenericPE):
    def __init__(self, threshold):
        GenericPE.__init__(self)
        self._add_input("matrix", grouping=[2])
        self._add_output("graph")
        self._add_output("clique")
        self.threshold = threshold
        # self.prov_cluster="myne"

        self.parameters = {"threshold": threshold}

        # Uncomment this line to associate this PE to the mycluster provenance-cluster
        # self.prov_cluster ='mycluster'

    def _process(self, inputs):
        if "matrix" in inputs:
            matrix = inputs["matrix"][0]
            batch = inputs["matrix"][1]

        low_values_indices = matrix < self.threshold  # Where values are low
        matrix[low_values_indices] = 0
        # self.log(matrix)
        self.log(batch)
        self.write("graph", matrix, metadata={"matrix": str(matrix), "batch": batch})
        self.write(
            "clique",
            matrix,
            metadata={"clique": str(matrix), "batch": batch},
            ignore_inputs=False,
        )

        G = nx.from_numpy_matrix(matrix)
        plt.figure(batch)
        nx.draw(G)
        fig1 = plt.gcf()
        plt.close(fig1)

        # H = nx.from_numpy_matrix(matrix)
        # plt.figure(2)
        # nx.draw(H)
        # plt.close()

        # Streams out values at 1/self.sr sampling rate, until iteration>0


class CompMatrix(GenericPE):
    def __init__(self, variables_number):
        GenericPE.__init__(self)

        self._add_output("output")
        self.size = variables_number
        self.parameters = {"variables_number": variables_number}
        self.data = {}

        # Uncomment this line to associate this PE to the mycluster provenance-cluster
        # self.prov_cluster ='mycluster'self.prov_cluster='mycluster'

    def _process(self, data):
        for x in data:
            if data[x][1] not in self.data:
                # prepares the data to visualise the xcor matrix of a specific batch number.
                self.data[data[x][1]] = {}
                self.data[data[x][1]]["matrix"] = np.identity(self.size)
                self.data[data[x][1]]["ro_count"] = 0

            self.data[data[x][1]]["matrix"][(data[x][2][1], data[x][2][0])] = data[x][0]
            self.addToProvState(
                "batch_" + str(data[x][1]),
                self.data[data[x][1]]["matrix"],
                metadata={"matrix": str(self.data[data[x][1]]["matrix"])},
                dep=["batch_" + str(data[x][1])],
                ignore_inputs=False,
            )
            self.data[data[x][1]]["ro_count"] += 1

            if self.data[data[x][1]]["ro_count"] == (self.size * (self.size - 1)) / 2:
                matrix = self.data[data[x][1]]["matrix"]

                d = pd.DataFrame(
                    data=matrix,
                    columns=range(self.size),
                    index=range(self.size),
                )

                mask = np.zeros_like(d, dtype=np.bool)
                mask[np.triu_indices_from(mask)] = True

                # Set up the matplotlib figure
                f, ax = plt.subplots(figsize=(11, 9))

                # Generate a custom diverging colormap
                cmap = sns.diverging_palette(220, 10, as_cmap=True)

                # Draw the heatmap with the mask and correct aspect ratio
                sns.heatmap(
                    d,
                    mask=mask,
                    cmap=cmap,
                    vmax=1,
                    square=True,
                    linewidths=0.5,
                    cbar_kws={"shrink": 0.5},
                    ax=ax,
                )

                # sns.plt.show()
                # self.log(matrix)
                self.write(
                    "output",
                    (matrix, data[x][1], self.name),
                    metadata={"matrix": str(d), "batch": str(data[x][1])},
                    dep=["batch_" + str(data[x][1])],
                )


class CorrCoef(GenericPE):
    def __init__(self, batch_size, index):
        GenericPE.__init__(self)
        self._add_input("input1", grouping=[0])
        self._add_input("input2", grouping=[0])
        self._add_output("output")
        self.index1 = 0
        self.index2 = 0
        self.batch1 = []
        self.batch2 = []
        self.size = batch_size
        self.parameters = {"batch_size": batch_size}
        self.index = index
        self.batchnum = 1

    def _process(self, inputs):
        val = None

        try:
            val = inputs["input1"][1]
            self.batch1.append(val)
            # self.log("Variables= "+str(inputs['input1'][0]))
            # if len(self.batch1)>=self.size:
            contributesto = (len(self.batch1) - 1) / self.size + self.batchnum
            # Umment to record entities in the Provenance State
            self.addToProvState(
                "batch1_" + str(contributesto),
                self.batch1,
                metadata={
                    "name": "batch1_" + str(contributesto),
                    "batch1": str(self.batch1),
                },
                ignore_inputs=False,
                dep=["batch1_" + str(contributesto)],
            )

        except KeyError:
            # traceback.print_exc(file=sys.stderr)
            val = inputs["input2"][1]
            self.batch2.append(val)
            # self.log("Variables= "+str(inputs['input2'][0]))
            # if len(self.batch2)>=self.size:

            contributesto = (len(self.batch2) - 1) / self.size + self.batchnum
            # Uncomment to record Element in the Provenance State
            self.addToProvState(
                "batch2_" + str(contributesto),
                self.batch2,
                metadata={
                    "name": "batch2_" + str(contributesto),
                    "batch2": str(self.batch2),
                },
                ignore_inputs=False,
                dep=["batch2_" + str(contributesto)],
            )

        # self.addToProvState(None,,ignore_dep=False)

        if len(self.batch2) >= self.size and len(self.batch1) >= self.size:
            array1 = np.array(self.batch1[0: self.size])
            array2 = np.array(self.batch2[0: self.size])
            ro = np.corrcoef([array1, array2])

            # stream out the correlation coefficient, the sequence number of the batch and the indexes of the sources.
            # Uncomment to reference entities in the Provenance State
            self.write(
                "output",
                (ro[0][1], self.batchnum, self.index, self.name),
                metadata={
                    "batchnum": self.batchnum,
                    "ro": ro[0][1],
                    "array1": str(array1),
                    "array2": str(array2),
                    "source_index": self.index,
                },
                dep=["batch1_" + str(self.batchnum), "batch2_" + str(self.batchnum)],
            )

            # Uncomment to reference entities in the Data Flow
            # self.write('output',(ro[0][1],self.batchnum,self.index),metadata={'batchnum':self.batchnum,'ro':str(ro[0][1]),'array1':str(array1),'array2':str(array2),'source_index':self.index})

            self.batchnum += 1
            # self.log(self.batchnum)

            self.batch1 = self.batch1[(self.size): len(self.batch1)]
            self.batch2 = self.batch2[(self.size): len(self.batch2)]


# number of projections = iterations/batch_size at speed defined by sampling rate
variables_number = 10
sampling_rate = 10000
batch_size = 20
iterations = 20

input_data = {"Start": [{"iterations": [iterations]}]}

# Instantiates the Workflow Components
# and generates the graph based on parameters


def createWf():
    graph = WorkflowGraph()
    mat = CompMatrix(variables_number)
    # mat.prov_cluster='record1'
    mc = MaxClique(-0.01)
    # mc.prov_cluster='record1'
    start = Start()
    # start.prov_cluster='record0'
    sources = {}
    mc.numprocesses = 1
    mat.numprocesses = 1

    for i in range(variables_number):
        sources[i] = Source(sampling_rate, i)
        # sources[i].prov_cluster='record0'
        #'+str(i%variables_number)
        # +str(i%7)
        sources[i].numprocesses = 1
        # sources[i].name="Source"+str(i)

    for h in range(variables_number):
        graph.connect(start, "output", sources[h], "iterations")
        for j in range(h + 1, variables_number):
            cc = CorrCoef(batch_size, (h, j))
            # cc.prov_cluster='record2'
            # +str(h%variables_number)

            mat._add_input("input" + "_" + str(h) + "_" + str(j), grouping=[3])
            graph.connect(sources[h], "output", cc, "input1")
            graph.connect(sources[j], "output", cc, "input2")
            graph.connect(cc, "output", mat, "input" + "_" + str(h) + "_" + str(j))
            cc.numprocesses = 1

    graph.connect(mat, "output", mc, "matrix")

    return graph


# from dispel4py.visualisation import display
# display(graph)

print("Preparing for: " + str(iterations / batch_size) + " projections")


# Store via sensors
ProvenanceRecorder.REPOS_URL = "http://127.0.0.1:8082/workflow/insert"

# Store via service
ProvenancePE.REPOS_URL = "http://127.0.0.1:8082/workflow/insert"

# Store to local path
ProvenancePE.PROV_PATH = os.environ["PROV_PATH"]

# Size of the provenance bulk before storage
ProvenancePE.BULK_SIZE = 100

# ProvenancePE.REPOS_URL='http://climate4impact.eu/prov/workflow/insert'


class ProvenanceSpecs(ProvenancePE):
    def __init__(self):
        ProvenancePE.__init__(self)
        self.streammeta = []
        self.count = 1

    def extractItemMetadata(self, data, port="output"):
        return {"this": data}


class ProvenanceOnWriteOnly(ProvenancePE):
    def __init__(self):
        ProvenancePE.__init__(self)
        self.streammeta = []
        self.count = 1

    def applyFlowResetPolicy(self, event, value):
        if event == "state":
            # self.log(event)
            self.provon = False

        super().applyFlowResetPolicy(event, value)
        # self.provon=False


class ProvenanceRecorderToService(ProvenanceRecorder):
    def __init__(self, name="ProvenanceRecorderToService", toW3C=False):
        ProvenanceRecorder.__init__(self)
        self.name = name
        self.numprocesses = 2
        self.convert_to_w3c = toW3C

    def _preprocess(self):
        self.provurl = urlparse(ProvenanceRecorder.REPOS_URL)
        self.connection = httplib.HTTPConnection(self.provurl.netloc)

    def send_to_service(self, prov):
        params = urllib.urlencode({"prov": ujson.dumps(prov)})
        headers = {
            "Content-type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        }
        self.connection.request("POST", self.provurl.path, params, headers)

        self.connection.getresponse()
        # self.log("Postprocress: " +
        #         str((response.status, response.reason, response,
        #             response.read())))
        self.connection.close()

    def process(self, inputs):
        try:
            for x in inputs:
                prov = inputs[x]

                if "_d4p" in prov:
                    prov = prov["_d4p"]
                    self.log(prov)

                self.send_to_service(prov)

        except:
            self.log(traceback.format_exc())


class ProvenanceRecorderToFile(ProvenanceRecorder):
    def __init__(self, name="ProvenanceRecorderToFile", toW3C=False):
        ProvenanceRecorder.__init__(self)
        self.name = name
        self.numprocesses = 3
        self.convertToW3C = toW3C

    def process(self, inputs):
        try:
            for x in inputs:
                # self.log(x)
                prov = inputs[x]

                # if isinstance(prov, list) and "data" in prov[0]:
                #    prov = prov[0]["data"]
                # el
                if "_d4p" in prov:
                    prov = prov["_d4p"]

                with open(os.environ["PROV_PATH"] + "/bulk_" + getUniqueId(), "wr") as filep:
                    ujson.dump(prov, filep)
                    filep.close()

        except:
            self.log(traceback.format_exc())


class ProvenanceSummaryToService(ProvenanceRecorderToService):
    def __init__(self, name="ProvenanceSummaryToService", to_w3c=False):
        ProvenanceRecorderToService.__init__(self)
        self.name = name
        # self.numprocesses=3
        self.convert_to_w3c = to_w3c
        self.doc_count = 0
        self.document = {}
        self.streamsstart = []
        self.streamsend = []
        self.document.update({"streams": [{"content": [{}, {}]}]})
        self.document.update({"startTime": None})
        self.document.update({"endTime": None})
        self.document.update({"derivationIds": []})
        self.document.update({"parameters": []})
        self.contente = []
        self.contents = []
        self.derivationIndex = {}
        self.content = []
        self.locationss = []
        self.locationse = []
        self.update = False

    def postprocess(self):
        if self.update > 0:
            self.send_to_service(self.document)

    def process(self, inputs):
        try:
            for x in inputs:
                prov = inputs[x]

            if isinstance(prov, list) and "data" in prov[0]:
                prov = prov[0]["data"]
            elif "_d4p" in prov:
                prov = prov["_d4p"]
                # self.log(x)
                self.send_to_service(prov)
                return
            elif "provenance" in prov:
                prov = prov["provenance"]

            if isinstance(prov, list):
                for x in prov:
                    self.doc_count += 1
                    # self.log(x)

                    for key in x:
                        if isinstance(x[key], list):
                            continue
                        if (
                            self.doc_count == 1
                            and (key != "startTime")
                            and (key != "endTime")
                        ):
                            self.document.update({key: x[key]})

                        self.document.update(
                            {"_id": x["prov_cluster"] + "_" + x["runId"]},
                        )
                        self.document.update(
                            {"instanceId": x["prov_cluster"] + "_" + x["runId"]},
                        )

                        #
                        if (self.document["startTime"] is None) or parse_date(
                            self.document["startTime"],
                        ) > parse_date(x["startTime"]):
                            # self.log("Adj  time to: "+str(x['endTime']))
                            self.document.update({"startTime": x["startTime"]})
                            self.streamsstart = x["streams"]

                        elif (self.document["endTime"] is None) or parse_date(
                            self.document["endTime"],
                        ) < parse_date(x["endTime"]):
                            self.document.update({"endTime": x["endTime"]})
                            self.streamsend = x["streams"]
                            self.document.update(x["parameters"])

                    for d in x["derivationIds"]:
                        if d["prov_cluster"] not in self.derivationIndex:
                            derivation = {
                                "DerivedFromDatasetID": "Data_"
                                + d["prov_cluster"]
                                + "_"
                                + self.document["runId"],
                            }
                            self.derivationIndex.update({d["prov_cluster"]: derivation})

            for d in self.streamsstart:
                if "location" in d and d["location"]:
                    self.locationss.append(d["location"])
                for c in d["content"]:
                    self.contents.append(c)

            for d in self.streamsend:
                if "location" in d and d["location"]:
                    self.locationse.append(d["location"])
                for c in d["content"]:
                    self.contente.append(c)

            if len(self.contents) > 0:
                self.update = True
                self.document["streams"][0]["content"][0] = self.contents
                self.document["streams"][0].update(
                    {
                        "id": "Data_"
                        + self.document["prov_cluster"]
                        + "_"
                        + self.document["runId"],
                        "location": self.locationss,
                    },
                )

            if len(self.contente) > 0:
                self.update = True
                self.document["streams"][0]["content"][1] = self.contente
                self.document["streams"][0].update(
                    {
                        "id": "Data_"
                        + self.document["prov_cluster"]
                        + "_"
                        + self.document["runId"],
                        "location": self.locationse,
                    },
                )

            self.document["streams"][0]["content"] = (
                self.document["streams"][0]["content"][0]
                + self.document["streams"][0]["content"][1]
            )

            for x in self.derivationIndex:
                self.document["derivationIds"].append(self.derivationIndex[x])

            if self.update:
                # Self.log(self.document)
                #    del  self.document['streamsstart']
                #    del  self.document['streamsend']
                self.send_to_service(self.document)
                self.update = False
                self.contente = []
                self.contents = []

            # for key in self.document:
            #    del key
            # self.document.update({'streamsstart':[]})
            # self.document.update({'streamsend':[]})
            #    self.document.update({'startTime':None})
            #    self.document.update({'endTime':None})
            #    self.document.update({'derivationIds':[]})

        except Exception as e:
            self.log(
                f"Dispel4Py ------> Exception {e},"
                f"Traceback: {traceback.format_exc()}",
            )


class ProvenanceRecorderToFileBulk(ProvenanceRecorder):
    def __init__(self, name="ProvenanceRecorderToFileBulk", to_w3c=False):
        ProvenanceRecorder.__init__(self)
        self.name = name
        self.numprocesses = 3
        self.convertToW3C = to_w3c
        self.bulk = []

    def postprocess(self):
        try:
            if len(self.bulk) > 0:
                with open(os.environ["PROV_PATH"] + "/bulk_" + getUniqueId(), "wr") as filep:
                    ujson.dump(self.bulk, filep)
                    filep.close()
                self.bulk[:] = []
        except Exception as e:
            self.log(
                f"Dispel4Py ------> Exception {e},"
                f"Traceback: {traceback.format_exc()}",
            )

    def process(self, inputs):
        try:
            for x in inputs:
                prov = inputs[x]

            if isinstance(prov, list) and "data" in prov[0]:
                prov = prov[0]["data"]
            elif "_d4p" in prov:
                prov = prov["_d4p"]

            self.bulk.append(prov)
            # self.log(os.environ['PBS_NODEFILE'])
            # self.log(socket.gethostname())
            if len(self.bulk) == 100:
                # ToDo: fix magic value
                with open(
                    os.environ["PROV_PATH"] + "/bulk_" + getUniqueId(), "wr",
                ) as filep:
                    ujson.dump(self.bulk, filep)
                    filep.close()

                self.bulk[:] = []
        #                for x in self.bulk:
        #                    del x
        except Exception as e:
            self.log(
                f"Dispel4Py ------> Exception {e},"
                f"Traceback: {traceback.format_exc()}",
            )


def create_graph_with_prov():
    return createWf()
    # Location of the remote repository for runtime updates of the lineage traces. Shared among ProvenanceRecorder
    # subtypes

    # Ranomdly generated unique identifier for the current run
    # os.environ["RUN_ID"]

    # Finally, provenance enhanced graph is prepared:

    # Initialise provenance storage in files:
    # InitiateNewRun(graph,None,provImpClass=(ProvenancePE,),componentsType={'CorrCoef':(ProvenancePE,)},username='aspinuso',runId=rid,w3c_prov=False,description="provState",workflowName="test_rdwd",workflowId="xx",save_mode='file')
    # skip_rules={'CorrCoef':{'ro':{'$lt':0}}})

    # Initialise provenance storage to service:
    # InitiateNewRun(graph,None,provImpClass=(ProvenancePE,),username='aspinuso',runId=rid,w3c_prov=False,description="provState",workflowName="test_rdwd",workflowId="xx",save_mode='service')
    # skip_rules={'CorrCoef':{'ro':{'$lt':0}}})

    # clustersRecorders={'record0':ProvenanceRecorderToFileBulk,'record1':ProvenanceRecorderToFileBulk,'record2':ProvenanceRecorderToFileBulk,'record6':ProvenanceRecorderToFileBulk,'record3':ProvenanceRecorderToFileBulk,'record4':ProvenanceRecorderToFileBulk,'record5':ProvenanceRecorderToFileBulk}
    # Initialise provenance storage to sensors and Files:
    # InitiateNewRun(graph,ProvenanceRecorderToFile,provImpClass=(ProvenancePE,),username='aspinuso',runId=rid,w3c_prov=False,description="provState",workflowName="test_rdwd",workflowId="xx",save_mode='sensor')
    # clustersRecorders=clustersRecorders)

    # Initialise provenance storage to sensors and service:
    # InitiateNewRun(graph,ProvenanceRecorderToService,provImpClass=(ProvenancePE,),username='aspinuso',runId=rid,w3c_prov=False,description="provState",workflowName="test_rdwd",workflowId="xx",save_mode='sensor')

    # Summary view on each component
    # InitiateNewRun(graph,ProvenanceSummaryToService,provImpClass=(ProvenancePE,),username='aspinuso',runId=rid,w3c_prov=False,description="provState",workflowName="test_rdwd",workflowId="xx",save_mode='sensor')

    # Initialise provenance storage end associate a Provenance type with specific components:
    # InitiateNewRun(graph,provImpClass=ProvenancePE,componentsType={'Source':(ProvenanceStock,)},username='aspinuso',runId=rid,w3c_prov=False,description="provState",workflowName="test_rdwd",workflowId="xx",save_mode='service')

    #


# .. and visualised..


import argparse

args = argparse.Namespace
args.num = 424
args.simple = False

num = 1


# print("PROV TO SENSOR")
print("PROV TO FILE")
# print("NO PROV")
graph = create_graph_with_prov()
# graph = createWf()
# global gtime
# gtime = time.time()

from dispel4py.visualisation import *

display(graph)
