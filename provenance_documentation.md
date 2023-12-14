# dispel4py

## ProvenanceType
```python
ProvenanceType(self)
```

A workflow is a program that combines atomic and independent processing elements
via a specification language and a library of components. More advanced systems
adopt abstractions to facilitate re-use of workflows across users' contexts and application
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

- _PROV_PATH_: When _SAVE_MODE_SERVICE_ is chosen, this variable should be populated with a string indcating a file system path wher the lineage will be stored
- _REPOS_URL_: When _SAVE_MODE_SERVICE_ is chosen, this variable should be populated with a string indcating the repository endpoint (S-ProvFlow) where the provenance will be sent.
- _PROV_DATA_EXPORT_URL: The service endpoint from where the provenance of a workflow execution, after being stored, can be extracted in PROV format.
- _BULK_SIZE_: Number of lineage documents to be stored in a single file or in a single request to the remote service. Helps tuning the overhead brough by the latency of accessing storage resources.


### getProvStateObjectId
```python
ProvenanceType.getProvStateObjectId(self, name)
```
Check if a data object with lookupterm _name_, is part of the provenance state (_s-prov:StateCollection_) and returns its _id_.

### makeProcessId
```python
ProvenanceType.makeProcessId(self)
```
Return the _id_ to be attributed to an running instance (_s-prov:ComponentInstance_) of a processing element.

### makeUniqueId
```python
ProvenanceType.makeUniqueId(self, data, output_port)
```
In support of the implementation of a _ProvenanceType_ realising a lineage __Contextualisation type__.
Return the _id_ to be attributed to a data entity (_s-prov:Data_) produced in output.

### apply_derivation_rule
```python
ProvenanceType.apply_derivation_rule(self, event, voidInvocation, oport=None, iport=None, data=None, metadata=None)
```
In support of the implementation of a _ProvenanceType_ realising a lineage _Pattern type_. This method is invoked by the _ProvenanceType_ each iteration when a decision has to be made whether to ignore or discard the dependencies on the ingested stream
and stateful entities, applying a specific provenance pattern, thereby creating input/output derivations. The framework invokes this method every time the data is written on an output port (_event_: _write_) and every
time an invocation (_s-prov:Invocation_) ends (_event_: _end_invocation_event_). The latter can be further described by  the boolean parameter _voidInvocation_, indicating whether the invocation terminated with any data produced.
The default implementation provides a _stateless_ behaviour, where the output depends only from the input data recieved during the invocation.


### getInputAt
```python
ProvenanceType.getInputAt(self, port='input', gindex=None)
```
Return input data currently available at a specific _port_. When reading input of a grouped operator, the _gindex_ parameter allows to access exclusively the data related to the group index.

### addNamespacePrefix
```python
ProvenanceType.addNamespacePrefix(self, prefix, url)
```
In support of the implementation of a _ProvenanceType_ realising a lineage _Contextualisation type_.
A Namespace _prefix_ can be declared with its vocabulary _url_ to map the metadata terms to external controlled vocabularies.
They can be used to qualify the metadata terms extracted from the _extractItemMetadata_ function,
as well as for those terms injected selectively at runtime by the _write_ method. The namespaces will be used
consistently when exporting the lineage traces to semantic-web formats, such as RDF.

### extractItemMetadata
```python
ProvenanceType.extractItemMetadata(self, data, port)
```
In support of the implementation of a _ProvenanceType_ realising a lineage _Contextualisation type_.
Extracts metadata from the domain specific content of the data (s-prov:DataGranules) written on a components output _port_, according to a particular vocabulary.

### ignorePastFlow
```python
ProvenanceType.ignorePastFlow(self)
```
In support of the implementation of a _ProvenanceType_ realising a lineage __Pattern type__.

It instructs the type to ignore the all the inputs when the method _apply_derivation_rule_ is invoked for a certain event."

### ignoreState
```python
ProvenanceType.ignoreState(self)
```
In support of the implementation of a _ProvenanceType_ realising a lineage __Pattern type__.

It instructs the type to ignore the content of the provenance state when the method _apply_derivation_rule_ is invoked for a certain event."

### discardState
```python
ProvenanceType.discardState(self)
```
In support of the implementation of a _ProvenanceType_ realising a lineage __Pattern type__.

It instructs the type to reset the data dependencies in the provenance state when the method _apply_derivation_rule_ is invoked for a certain event.
These will not be availabe in the following invocations."

### discardInFlow
```python
ProvenanceType.discardInFlow(self, wlength=None, discardState=False)
```
In support of the implementation of a _ProvenanceType_ realising a lineage __Pattern type__.

It instructs the type to reset the data dependencies related to the component''s inputs when the method _apply_derivation_rule_ is invoked for a certain event.
These will not be availabe in the following invocations."

### update_prov_state
```python
ProvenanceType.update_prov_state(self, lookupterm, data, location='', format='', metadata={}, ignore_inputs=False, ignore_state=True, **kwargs)
```
In support of the implementation of a _ProvenanceType_ realising a lineage _Pattern type_ or inn those circumstances where developers require to explicitly manage the provenance information within the component''s logic,.

Updates the provenance state (_s-prov:StateCollection_) with a reference, identified by a _lookupterm_, to a new _data_ entity or to the current input. The _lookupterm_ will allow developers to refer to the entity when this is used to derive new data.
Developers can specify additional _medatata_ by passing a metadata dictionary. This will enrich the one generated by the _extractItemMetadata_ method.
Optionally the can also specify _format_ and _location_ of the output when this is a concrete resource (file, db entry, online url), as well as instructing the provenance generation to 'ignore_input' and 'ignore_state' dependencies.

The _kwargs_ parameter allows to pass an argument _dep_ where developers can specify a list of data _id_ to explicitly declare dependencies with any data in the provenance state (_s-prov:StateCollection_).

### write
```python
ProvenanceType.write(self, name, data, **kwargs)
```

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

### checkSelectiveRule
```python
ProvenanceType.checkSelectiveRule(self, streammeta)
```
In alignement with what was previously specified in the configure_prov_run for the Processing Element,
check the data granule metadata whether its properies values fall in a selective provenance generation rule.

### checkTransferRule
```python
ProvenanceType.checkTransferRule(self, streammeta)
```
In alignement with what was previously specified in the configure_prov_run for the Processing Element,
check the data granule metadata whether its properies values fall in a selective data transfer rule.

### extractDataSourceId
```python
ProvenanceType.extractDataSourceId(self, data, port)
```
In support of the implementation of a _ProvenanceType_ realising a lineage _Pattern type_. Extract the id from the incoming data, if applicable,
to reuse it to identify the correspondent provenance entity. This functionality is handy especially when a workflow component ingests data represented by
self-contained and structured file formats. For instance, the NetCDF attributes Convention includes in its internal metadata an id that can be reused to ensure
the linkage and therefore the consistent continuation of provenance tracesbetween workflow executions that generate and use the same data.

## AccumulateFlow
```python
AccumulateFlow(self)
```
A _Pattern type_ for a Processing Element (_s-prov:Component_) whose output depends on a sequence of input data; e.g. computation of periodic average.

## Nby1Flow
```python
Nby1Flow(self)
```
A _Pattern type_ for a Processing Element (_s-prov:Component_) whose output depends
on the data received on all its input ports in lock-step; e.g. combined analysis of multiple
variables.

## SlideFlow
```python
SlideFlow(self)
```
A _Pattern type_ for a Processing Element (_s-prov:Component_) whose output depends
on computations over sliding windows; e.g. computation of rolling sums.

## ASTGrouped
```python
ASTGrouped(self)
```
A _Pattern type_ for a Processing Element (_s-prov:Component_) that manages a stateful operator
with grouping rules; e.g. a component that produces a correlation matrix with the incoming
coefficients associated with the same sampling-iteration index

## SingleInvocationFlow
```python
SingleInvocationFlow(self)
```
A _Pattern type_ for a Processing Element (_s-prov:Component_) that
presents stateless input output dependencies; e.g. the Processing Element of a simple I/O
pipeline.

## AccumulateStateTrace
```python
AccumulateStateTrace(self)
```
A _Pattern type_ for a Processing Element (_s-prov:Component_) that
keeps track of the updates on intermediate results written to the output after a sequence
of inputs; e.g. traceable approximation of frequency counts or of periodic averages.

## IntermediateStatefulOut
```python
IntermediateStatefulOut(self)
```
A _Pattern type_ for a Processing Element (_s-prov:Component_) stateful component which produces distinct but interdependent
output; e.g. detection of events over periodic observations or any component that reuses the data just written to generate a new product

## ForceStateless
```python
ForceStateless(self)
```
A _Pattern type_ for a Processing Element (_s-prov:Component_). It considers the outputs of the component dependent
only on the current input data, regardless from any explicit state update; e.g. the user wants to reduce the
amount of lineage produced by a component that presents inline calls to the _update_prov_state_, accepting less accuracy.

## get_source
```python
get_source(object, spacing=10, collapse=1)
```
Print methods and doc strings.

Takes module, class, list, dictionary, or string.
## configure_prov_run
```python
configure_prov_run(graph, provRecorderClass=None, provImpClass=<class 'dispel4py.provenance_doc.ProvenanceType'>, input=None, username=None, workflowId=None, description=None, system_id=None, workflowName=None, workflowType=None, w3c_prov=False, runId=None, componentsType=None, clustersRecorders={}, feedbackPEs=[], save_mode='file', sel_rules={}, transfer_rules={}, update=False)
```
To enable the user of a data-intensive application to configure the attribution
of types, selectivity controls and activation of advanced exploitation mechanisms, we
introduce the concept of provenance configuration. With the configuration users can specify a number of properties, such as attribution,
provenance types, clusters, sensors, selectivity rules, etc. The configuration is
used at the time of the initialisation of the workflow to prepare its provenance-aware
execution. We consider that a chosen configuration may be influenced by personal and
community preferences, as well as by rules introduced by institutional policies. For
instance, a Research Infrastructure (RI) may indicate best practices to reproduce and
describe the operations performed by the users exploiting its facilities, or even impose
requirements which may turn into quality assessment metrics.
This could require to choose among a set of contextualisation types, in order to adhere to
the infrastructure's metadata portfolio. Thus, a provenance configuration profile play
in favour of more generality, encouraging the implementation and the re-use of fundamental
methods across disciplines.

With this method, the users of the workflow provide general provenance information on the attribution of the run, such as _username_, _runId_ (execution id),
_description_, _workflowName_, and its semantic characterisation _workflowType_. It allows users to indicate which provenance types to apply to each component
and the belonging conceptual provenance cluster. Moreover, users can also choose where to store the lineage (_save_mode_), locally in the file system or in a remote service or database.
Lineage storage operations can be performed in bulk, with different impacts on the overall overhead and on the experienced rapidity of access to the lineage information.



- __Selectivity and Transfer rules__: By declaratively indicating a set of Selectivity and Transfer rules for every component (_sel_rules_, _transfer_rules_), users can respectively activate the collection
of the provenance for particular Data elements or trigger transfer operations of the data to external locations. The approach takes advantage of the contextualisation
possibilities offered by the provenance _Contextualisation types_. The rules consist of comparison expressions formulated in JSON that indicate the boundary
values for a specific metadata term. Such representation is inspired by the query language and selectors adopted by a popular document store, MongoDB.

Example, a Processing Element _CorrCoef_ that produces lineage information only when the _rho_ value is greater than 0:
```python
    { "CorrCoef": {
        "rules": {
            "rho": {
                "$gt": 0
    }}}}
```


## ProvenanceSimpleFunctionPE
```python
ProvenanceSimpleFunctionPE(self, *args, **kwargs)
```
A _Pattern type_ for the native  _SimpleFunctionPE_ of dispel4py

## ProvenanceIterativePE
```python
ProvenanceIterativePE(self, *args, **kwargs)
```
A _Pattern type_ for the native  _IterativePE_ Element of dispel4py

