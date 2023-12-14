# import libraries
import math
import re

import pandas as pd


# This function is to get the communication time among PEs
def communication_time(df):
    write_logs = df.loc[df["method"] == "write"][["PE", "rank", "start", "end", "data"]]
    wcols = (
        write_logs["data"]
        .apply(eval)
        .map(lambda d: [d["id"], d["size"]])
        .apply(pd.Series)
    )
    write_logs["data_id"] = wcols[0]
    write_logs["data_size"] = wcols[1]
    write_logs = write_logs.drop(columns=["data"])

    read_logs = df.loc[df["method"] == "read"][["PE", "rank", "start", "end", "data"]]
    rcols = read_logs["data"].apply(eval).map(lambda d: d["input"]).apply(pd.Series)
    read_logs["data_id"] = rcols[0].map(
        lambda t: t[1] if isinstance(t, tuple) else None,
    )
    read_logs = read_logs[read_logs["data_id"].notnull()]
    read_logs = read_logs.drop(columns=["data"])

    jdf = read_logs.set_index("data_id").join(
        write_logs.set_index("data_id"),
        lsuffix="_r",
        rsuffix="_w",
    )
    jdf["t_comm"] = jdf[["end_w", "end_r"]].max(axis=1) - jdf[
        ["start_r", "start_w"]
    ].max(axis=1)
    communication_times = jdf[["PE_w", "rank_w", "PE_r", "rank_r", "t_comm"]]
    return (
        communication_times[["PE_w", "PE_r", "t_comm"]].group_by(["PE_w", "PE_r"]).max()
    )


# This function is to get the processing time of each PE
def processing_times(df):
    pl = df.loc[df["method"] == "process"][["PE", "rank", "start", "end"]]
    pl["t_proc"] = pl["end"] - pl["start"]
    processing_times = pl.drop(columns=["start", "end"])
    return processing_times[["PE", "t_proc"]].group_by(["PE"]).max()


# This function is to check if two PEs should be placed one partition or not
def create_partitions(i, ct, pt):
    partitions = []

    if i != len(ct) - 1:
        source = ct.index[i][0]
        dest = ct.index[i][1]

        exc_time = min(pt.loc[source, "t_proc"], pt.loc[dest, "t_proc"])
        com_time = ct.iloc[i, 0]

        # communication time between two PEs is larger than the minimum processing time,
        # then put these two PEs into one partition
        if com_time > exc_time:
            partitions = [dest, source]

            p2 = create_partitions(i + 1, ct, pt)

            partitions = partitions + p2

    return list(set(partitions))


# This function is to create partitions for all PEs based on processing time and communication time
def partition_all_pes(ct, pt):
    partitions_list = []

    pe_partition = {}
    for i in range(len(ct) - 1):
        source = ct.index[i][0]
        dest = ct.index[i][1]

        if source not in [
            item for sublist in pe_partition.values() for item in sublist
        ]:
            partitions = create_partitions(i, ct, pt)

            if partitions != []:
                pe_partition[dest] = partitions

            else:
                pe_partition[source] = [source]
                if dest not in [
                    item for sublist in pe_partition.values() for item in sublist
                ]:
                    pe_partition[dest] = [dest]

    source_pe = ct.index[len(ct) - 1][0]
    pe_partition[source_pe] = [source_pe]

    for sublist in pe_partition.values():
        partitions_list.append(sublist)

    return partitions_list


# This function is to normalize the number of assigned processors,
# since sometimes the total number of assigned processors are larger than the set processors
def normalize(a, num_processor):
    if sum(a.values()) <= num_processor:
        return a

    else:
        for key, value in a.items():
            if value == max(a.values()):
                a[key] -= 1

        if sum(a.values()) > num_processor:
            a = normalize(a, num_processor)

    return a


# This function is to map partitions for a workflow and assign processors to each PE
def map_partitions(filename):
    df = pd.read_csv(filename)
    ct = communication_time(df)
    pt = processing_times(df)

    ct["order"] = [int(re.findall(r"\D+(\d+)", x)[0]) for x in ct.reset_index().PE_w]
    order_ct = ct.sort_values(by="order", ascending=False)

    partitions_list = partition_all_pes(order_ct, pt)

    source_pe = order_ct.index[len(order_ct) - 1][0]
    # HACK: change the name of the workflow
    num_processor = int(re.findall(r"corr_.+_n(\d+)_?", filename)[0])

    partitions_proc = {}

    for index in range(len(partitions_list)):
        partition = partitions_list[index]

        if len(partition) == 1 and partition[0] == source_pe:
            partitions_proc[index] = 1

        else:
            total_time = pt[pt.index != source_pe].sum()[0]

            exc_time = 0
            for pe in partition:
                exc_time = exc_time + pt.loc[pe, "t_proc"]

            num = math.ceil((num_processor - 1) * (exc_time / total_time))
            partitions_proc[index] = int(num)

    partitions_proc = normalize(partitions_proc, num_processor)

    return partitions_list, partitions_proc


# Important
# You need to specify the CSV to use here. This CSV is obtained using the --monitoring flag when the workflow was run
# Attention to the naming format of the file: workflow_mapping_n[NUMPROCESSES]_platform.csv
# Example:
# map_partitions('corr_multi_n32.csv')
