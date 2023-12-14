import seaborn as sns

from dispel4py.base import (
    GenericPE,
)
from dispel4py.new.processor import *
from dispel4py.provenance import *
from dispel4py.seismo.seismo import *
from dispel4py.workflow_graph import WorkflowGraph

sns.set(style="white")


class Read(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input("input")
        self._add_output("xarray")
        self.count = 0

    def _process(self, inputs):
        self.log("Read_Process")

        self.log(inputs)

        input_location = inputs["input"][0]

        ds = xarray.open_dataset(input_location)

        self.write("xarray", (ds, self.count), location=input_location)
        self.count += 1


class Write(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input("input")
        self._add_output("location")
        self.count = 0

    def _process(self, inputs):
        self.log("Write_Function")
        # self.log(inputs)
        output_location = "data/new_" + str(self.count) + ".nc"
        self.count += 1
        inputs["input"][0].to_netcdf(output_location)
        # self.log(output_location)
        self.write("location", output_location, location=output_location)


class Analysis(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input("input")
        self._add_output("output")

    def _process(self, inputs):
        self.log("Workflow_process")
        # self.log( len(inputs))

        # nc = inputs['input'][0]

        nc = inputs["input"][0]
        # self.log(nc)
        #
        self.write(
            "output",
            (nc, inputs["input"][1]),
            metadata={"prov:type": "clipc:sum", "index": inputs["input"][1]},
        )


class Combine(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)
        self._add_input("combine1", grouping=[1])
        self._add_input("combine2", grouping=[1])
        self._add_output("combo")
        self.count = 0
        self.data1 = []
        self.data2 = []
        self.nc1 = None
        self.nc2 = None
        self.out = None

    def _process(self, inputs):
        self.log("Combine_process")
        self.log(inputs.keys())

        if "combine1" in inputs:
            self.data1.append(inputs["combine1"][0])

        if "combine2" in inputs:
            self.data2.append(inputs["combine2"][0])

        if len(self.data1) > 0 and len(self.data2) > 0:
            # self.log("LEN"+str(len(self.data1)))
            nc1 = self.data1.pop(0)
            nc2 = self.data2.pop(0)

            nc = nc1
            # numpy arithmetic for DataArrays...

            # self.log(nc2)
            for k, v in nc2.attrs.items():
                if k in nc.attrs:
                    nc.attrs[k] = v
                else:
                    nc.attrs[k] = v

            self.write("combo", (nc, self.count))
            self.count += 1


####################################################################################################

# Declare workflow inputs: (each iteration prduces a batch_size of samples at the specified sampling_rate)
# number of projections = iterations/batch_size at speed defined by sampling rate
variables_number = 40
sampling_rate = 100
batch_size = 5
iterations = 3

input_data = {"Start": [{"iterations": [iterations]}]}

# Instantiates the Workflow Components
# and generates the graph based on parameters


# Instantiates the Workflow Components
# and generates the graph based on parameters


def create_wf():
    graph = WorkflowGraph()
    mat = CorrMatrix(variables_number)
    mat.prov_cluster = "record2"
    mc = MaxClique(-0.01)
    mc.prov_cluster = "record0"
    start = Start()
    start.prov_cluster = "record0"
    sources = {}

    cc = CorrCoef()
    cc.prov_cluster = "record1"
    stock = ["NASDAQ", "MIBTEL", "DOWJ"]

    for i in range(1, variables_number + 1):
        if i == 1:
            print("CC")
            sources[i] = Source(sampling_rate / 50, i, batch_size)
        else:
            sources[i] = Source(sampling_rate, i, batch_size)
        sources[i].prov_cluster = stock[i % len(stock) - 1]
        sources[i].numprocesses = 1
        # sources[i].name="Source"+str(i)

    for h in range(1, variables_number + 1):
        cc._add_input("input" + "_" + str(h + 1), grouping=[0])
        graph.connect(start, "output", sources[h], "iterations")
        graph.connect(sources[h], "output", cc, "input" + "_" + str(h + 1))

    graph.connect(cc, "output", mat, "input")
    graph.connect(mat, "output", mc, "input")

    return graph


print("Preparing for: " + str(iterations / batch_size) + " projections")


# Store via recorders or sensors
# ProvenanceRecorder.REPOS_URL='http://127.0.0.1:8080/j2ep-1.0/prov/workflow/insert'


# Store via recorders or sensors
# ProvenanceRecorder.REPOS_URL='http://127.0.0.1:8080/j2ep-1.0/prov/workflow/insert'

# Store via service
ProvenancePE.REPOS_URL = "http://127.0.0.1:8082/workflow/insert"

# Export data lineage via service (REST GET Call on dataid resource)
ProvenancePE.PROV_EXPORT_URL = "http://127.0.0.1:8082/workflow/export/data/"

# Store to local path
ProvenancePE.PROV_PATH = "./prov-files/"

# Size of the provenance bulk before sent to storage or sensor
ProvenancePE.BULK_SIZE = 20

# ProvenancePE.REPOS_URL='http://climate4impact.eu/prov/workflow/insert'

selrule1 = {"CorrCoef": {"rules": {"rho": {"$gt": 0}}}}

selrule2 = {"Start": {"rules": {"iterations": {"$lt": 0}}}}


def create_graph_with_prov():
    graph = create_wf()
    # Location of the remote repository for runtime updates of the lineage traces. Shared among ProvenanceRecorder subtypes

    # Ranomdly generated unique identifier for the current run
    rid = "CORR_LARGE_" + getUniqueId()

    # Finally, provenance enhanced graph is prepared:

    # Initialise provenance storage end associate a Provenance type with specific components:
    configure_prov_run(
        graph,
        None,
        provImpClass=(ProvenancePE,),
        username="aspinuso",
        runId=rid,
        input=[
            {"name": "variables_number", "url": variables_number},
            {"name": "sampling_rate", "url": sampling_rate},
            {"name": "batch_size", "url": batch_size},
            {"name": "iterations", "url": iterations},
        ],
        w3c_prov=False,
        description="provState",
        workflowName="test_rdwd",
        workflowId="!23123",
        componentsType={
            "MaxClique": {
                "s-prov:type": (IntermediateStatefulOut,),
                "state_dep_port": "graph",
                "s-prov:prov-cluster": "knmi:stockAnalyser",
            },
            "CorrMatrix": {
                "s-prov:type": (ASTGrouped,),
                "s-prov:prov-cluster": "knmi:stockAnalyser",
            },
            "CorrCoef": {
                "s-prov:type": (SingleInvocationFlow,),
                "s-prov:prov-cluster": "knmi:Correlator",
            },
        },
        save_mode="service",
        sel_rules=selrule2,
    )

    #
    return graph


# graph=createWf()
graph = create_graph_with_prov()
