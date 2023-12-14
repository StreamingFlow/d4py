"""
Usgae:

To import and call map_partitions in the workflow, then
graph.partitions = map_partitions(graph)

"""


# This function is to find one to one relationship for a certain PE
def find_one_to_one(obj, source_pe_dict, dest_pe_dict):
    part = []

    if obj in source_pe_dict:
        direct = source_pe_dict[obj]
        if len(direct) == 1 and len(dest_pe_dict[direct[0][1]]) == 1:
            if not check_grouping(direct[0][1]):
                part.extend((obj, direct[0][1]))

                part_2 = find_one_to_one(direct[0][1], source_pe_dict, dest_pe_dict)
                for x in part_2:
                    if x not in part:
                        part.append(x)

    return part


# This function is to check whether the PE has grouping parameter
def check_grouping(obj):
    return "grouping" in next(iter(obj.inputconnections.values()))


# This function is to map partitions for the whole graph
def map_partitions(graph):
    ### create source pe dictionary and destination pe dictionary
    source_pe_dict = {}
    dest_pe_dict = {}

    for node in graph.graph.nodes():
        edges = graph.graph.edges(node, data=True)

        source_direction_list = []
        dest_direction_list = []

        for edge in edges:
            source_pe = edge[2]["DIRECTION"][0]
            dest_pe = edge[2]["DIRECTION"][1]

            if node.get_contained_object() == source_pe:
                source_direction_list.append(edge[2]["DIRECTION"])
                source_pe_dict[source_pe] = source_direction_list

            if node.get_contained_object() == dest_pe:
                dest_direction_list.append(edge[2]["DIRECTION"])
                dest_pe_dict[dest_pe] = dest_direction_list

    ### find source pe of the whole graph
    source_partition = [x for x in source_pe_dict if x not in dest_pe_dict]
    if len(source_partition) != 1:
        raise AttributeError("Illegal Number of Source PE")
    else:
        del source_pe_dict[source_partition[0]]

    ### map other pes to partition
    partitions = [
        source_partition,
    ]
    pe_part = {}

    ### make partitions according to the source pe dictionary
    for obj in source_pe_dict:
        if obj not in [item for sublist in pe_part.values() for item in sublist]:
            part = find_one_to_one(obj, source_pe_dict, dest_pe_dict)
            if part != []:
                pe_part[obj] = part
            else:
                pe_part[obj] = [obj]

    for obj in dest_pe_dict:
        if obj not in [item for sublist in pe_part.values() for item in sublist]:
            pe_part[obj] = [obj]

    for obj, part in pe_part.items():
        partitions.append(part)

    return partitions
