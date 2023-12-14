

# ## Enabling Active-Provenance in dispel4py - How To:
# 
# 
# 
# ### Sample Workflow Description and Components (Compoenents are implemented as Classe or as Functions)
# <br/>
# 
# <li>1 - Produces a stream of numbers - Source (Class)</li>
# <li>2 - Calculates the square-product for each number - square (fuction)</li>
# <li>3 - Streams out the cartesian product of (numbers X square-products) - CrossProd (Class)</li>
# <li>4 - Divides (square-product/number) for each incoming element of the cartesian product - Div (Class)</li>
# 
# The script below defines the components and declares the workflow. Its execution will show a visual representation of the abstract workfkow grap.
# 
# While most of the processing elements are stateless, the <i>CrossProd</i> Class shows how to use the <i>write</i> method and the <i>addToState</i> to precisely trace stateful dependencies, in the case of the generation of a cross-product output.
# 
# ### Function addToProvState 
# Adds an object and its metadata to the PEs state. This can be referenced from the user during write operations, increasing the lineage precision in stateful components.
# 
# The accepted parameters are the following:
# 
# #### Unnamed parameters:
# <li> 1 - <i>data</i>: object to be stored in the provenance state</li>
# 
# #### Named Parameters:
# <li> 2 - <i>location</i>: url or path indicating the location of the data file, if any has been produced</li>
# <li> 3 - <i>metadata</i>: dictionary of key,values pairs od user-defined metadata associated to the object.</li>
# <li> 4 - <i>ignore_dep</i>: If <b>True</b> the dependencies which are currently standing are ignored, default True</li>
# <li> 5 - <i>stateless</i>:  If <b>True</b> the item added is not included as new standing dependencies, default True</li>
# 
# 
# 
# <br/>
# 

# In[1]:

from dispel4py.workflow_graph import WorkflowGraph 
from dispel4py.provenance import *
import time
import random
import traceback 
from dispel4py.base import create_iterative_chain, GenericPE, ConsumerPE, IterativePE, SimpleFunctionPE
from dispel4py.new.simple_process import process_and_return
from dispel4py.visualisation import display



class Source(GenericPE):

    def __init__(self):
        GenericPE.__init__(self)
        self._add_input("input")
        self._add_output("output")
        
        #Uncomment this line to associate this PE to the mycluster provenance-cluster 
        #self.prov_cluster ='mycluster'
        
    
    def _process(self, inputs):
        print inputs
    
        if "input" in inputs:
            #self.log('from input')
            inp=inputs["input"][0]
       
        #Strarting from the number received in input, streams out values until >0
        while (inp>0):
            self.write("output", inp, metadata={"iter": inp})
            inp=inp-5
        
        
        


def square(data, prov_cluster):
    data=data*data
    
    #User-defined metadata and format associated to the function output.
    prov={"format": "Random float", "metadata": {"value_s": str(data)}}
    
    return {"_d4p_prov": prov, "_d4p_data": data} 
    
    
    
class Div(GenericPE):

    def __init__(self):
        GenericPE.__init__(self)
        self._add_input("input")
        self._add_output("output")
        
        #Uncomment this line to associate this PE to the mycluster provenance-cluster 
        #self.prov_cluster ='mycluster'self.prov_cluster='mycluster'
            
    def _process(self, data):
        self.log("DIIIIV: "+str(data)) 
        val = data["input"][0]/data["input"][1]
        self.write("output", val, metadata={"val": val})
             
            
            
            
            
class CrossProd(GenericPE):

    def __init__(self):
        GenericPE.__init__(self)
        self._add_input("input1")
        self._add_input("input2")
        self._add_output("output")
        self.index1=0
        self.index2=0
        self.indexmap1={}
        self.indexmap2={}
         
         
        
    def _process(self, inputs):
        index=None
        val=None
        
         
            
        try:
            val = inputs["input1"]
            self.indexmap1[self.index1]=val
            
            #Adds the object and the associated metadata to the PEs state for later reference
            self.addToProvState(str("index1"+str(self.index1)), self.indexmap1[self.index1], metadata={"res": str(self.indexmap1[self.index1])}, ignore_inputs=False)
            self.index1+=1
            
            for x in self.indexmap2:
                
                # Writes the output and specify additional dependencies. 
                # Specifically, it refer to the object read from the list used to produce 
                # the cross product
                self.write("output", (val, self.indexmap2[x]), metadata={"res": str((val, self.indexmap2[x]))}, dep=[str("index2"+str(x))])
            
        except KeyError:
             
            val = inputs["input2"]
            self.indexmap2[self.index2]=val
            
            #Adds the object and the associated metadata to the PEs state for later reference
            self.addToProvState("index2"+str(self.index2), self.indexmap2[self.index2], metadata={"res": str(self.indexmap2[self.index2])}, ignore_inputs=False)
            self.index2+=1
            
            for x in self.indexmap1:
                
                # Writes the output and specify additional dependencies. 
                # Specifically, it refer to the object read from the list used to produce 
                # the cross product
                self.write("output", (val, self.indexmap1[x]), metadata={"res": str((val, self.indexmap1[x]))}, dep=[str("index1"+str(x))])
        
       
# Instantiates the Workflow Components        

sc = Source()
sc.name="PE_source"
divf=Div()
divf.name="PE_div"
crossp = CrossProd()
squaref=SimpleFunctionPE(square, {})
#Uncomment this line to associate this PE to the mycluster provenance-cluster 
squaref=SimpleFunctionPE(square, {"prov_cluster": "mycluster"})


#Initialise and compose the workflow graph
graph = WorkflowGraph()
graph.connect(sc, "output", squaref, "input")
graph.connect(sc, "output", crossp, "input1")
graph.connect(squaref, "output", crossp, "input2")
graph.connect(crossp, "output", divf, "input")


#Declare workflow inputs:
input_data = {"PE_source": [{"input": [10]}]}

# In[2]:

#Location of the remote repository for runtime updates of the lineage traces. Shared among ProvenanceRecorder subtypes
ProvenanceRecorder.REPOS_URL="http://localhost/prov/workflow/insert"




class ProvenanceRecorderToService(ProvenanceRecorder):

    def __init__(self, name="ProvenanceRecorderToService", toW3C=False):
        ProvenanceRecorder.__init__(self)
        self.name = name
        self.convertToW3C = toW3C
        # self.inputconnections[ProvenanceRecorder.INPUT_NAME] = {
        # "name": ProvenanceRecorder.INPUT_NAME}

    def _preprocess(self):
        self.provurl = urlparse(ProvenanceRecorder.REPOS_URL)
        self.connection = httplib.HTTPConnection(
            self.provurl.netloc)

    def _process(self, inputs):
        
        #ports are assigned automatically as numbers, we just need to read from any of these
        for x in inputs:
            prov = inputs[x]
            
        out = None
        if isinstance(prov, list) and "data" in prov[0]:
            prov = prov[0]["data"]

        if self.convertToW3C:
            out = toW3Cprov(prov)
        else:
            out = prov

        params = urllib.urlencode({"prov": json.dumps(out)})
        headers = {
            "Content-type": "application/x-www-form-urlencoded",
            "Accept": "application/json"}
        self.connection.request(
            "POST",
            self.provurl.path,
            params,
            headers)

        response = self.connection.getresponse()
        print("Response From Provenance Serivce: ", response.status,
              response.reason, response, response.read())
        self.connection.close()
        return None

    def postprocess(self):
        self.connection.close()



# ### MyProvenanceRecorderWithFeedback
# 
# Recieves traces from the PEs and reads its content. Depending from the 'name' of the PE sending the lineage, feedbacks are prepared and sent back.

# In[ ]:


class MyProvenanceRecorderWithFeedback(ProvenanceRecorder):

    def __init__(self, toW3C=False):
        ProvenanceRecorder.__init__(self)
        self.convertToW3C = toW3C
        self.bulk = []
        self.timestamp = datetime.datetime.utcnow()

    def _preprocess(self):
        self.provurl = urlparse(ProvenanceRecorder.REPOS_URL)

        self.connection = httplib.HTTPConnection(
            self.provurl.netloc)

    def postprocess(self):
        self.connection.close()
        
    def _process(self, inputs):
        prov = None
        for x in inputs:
            prov = inputs[x]
        out = None
        if isinstance(prov, list) and "data" in prov[0]:
            prov = prov[0]["data"]

        if self.convertToW3C:
            out = toW3Cprov(prov)
        else:
            out = prov

            
            
        self.write(self.porttopemap[prov["name"]], "FEEDBACK MESSAGGE FROM RECORDER")

        self.bulk.append(out)
        params = urllib.urlencode({"prov": json.dumps(self.bulk)})
        headers = {
            "Content-type": "application/x-www-form-urlencoded",
            "Accept": "application/json"}
        self.connection.request(
            "POST", self.provurl.path, params, headers)
        response = self.connection.getresponse()
        self.log("progress: " + str((response.status, response.reason,
                                         response, response.read())))
        self.bulk=[]
        

        return None


# ## Preparing the workflow graph for active provenance with feedback
# Here we show how to implement a PE that can handle feedback from a <i>ProvenanceRecoder</i>. We redefine the worklow graph with the same structure as the provious one but with the new PE
# 
# ### Class DivFeedback
# It requires the implementation of the <i>_process_feedbak</i> function. The function will be invoked automatically by the framework when a feedback is sent.
# <br/>

# In[4]:

class DivFeedback(GenericPE):

    def __init__(self):
        GenericPE.__init__(self)
        self._add_input("input")
        self._add_output("output")
        
        #Uncomment this line to associate this PE to the mycluster provenance-cluster 
        #self.prov_cluster ='mycluster'self.prov_cluster='mycluster'
         
         
        
    def _process_feedback(self, data):
        print "FEEEEDBACK: "+str(data)
    
    def _process(self, data):
        self.log("DIIIIV: "+str(data)) 
        val = data["input"][0]/data["input"][1]
        self.write("output", val, metadata={"val": val})
# Instantiates the Workflow Components        

sc = Source()
sc.name="PE_source"
divf=DivFeedback()
divf.name="PE_DivFeedback"
divf.prov_cluster="mycluster"
crossp = CrossProd()
#squaref=SimpleFunctionPE(square,{})
#Uncomment this line to associate this PE to the mycluster provenance-cluster 
squaref=SimpleFunctionPE(square, {"prov_cluster": "mycluster"})


#Initialise and compose the workflow graph
graph = WorkflowGraph()
graph.connect(sc, "output", squaref, "input")
graph.connect(sc, "output", crossp, "input1")
graph.connect(squaref, "output", crossp, "input2")
graph.connect(crossp, "output", divf, "input")


#Declare workflow inputs:
input_data = {"PE_source": [{"input": [10]}]}

# Preparing the workflow graph for provenance production, pre-analysis and storage
# Ranomdly generated unique identifier for the current run
rid="RDWD_"+getUniqueId()

# if ProvenanceRecorderToFile is used, this path will contains all the resulting JSON documents
os.environ["PROV_PATH"]="./prov-files/"

# Finally, provenance enhanced graph is prepared:
InitiateNewRun(graph, ProvenanceRecorderToFileBulk, provImpClass=ProvenancePE, username="aspinuso", runId=rid, w3c_prov=False, workflowName="test_rdwd", workflowId="xx", clustersRecorders={"mycluster": ProvenanceRecorderToFileBulk}, feedbackPEs=["PE_DivFeedback"])



# In[ ]:



