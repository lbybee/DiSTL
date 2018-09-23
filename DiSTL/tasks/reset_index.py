from .DTM import DTMTask
import pandas as pd
import multiprocessing
import luigi


def _map_count(count, term_id_map, doc_id_map):
    """internal function for applying new ids to count

    Parameters
    ----------
    count : pd DataFrame
        containing counts
    term_id_map : pd Series
        map from old term id to new term id
    doc_id_map : pd Series
        map from old doc id to new doc id

    Returns
    -------
    updated count
    """

    count["term_id"] = count["term_id"].map(term_id_map)
    count["doc_id"] = count["doc_id"].map(doc_id_map)
    count = count[~pd.isnull(count["doc_id"])]
    count = count[~pd.isnull(count["term_id"])]
    count["doc_id"] = count["doc_id"].astype(int)
    count["term_id"] = count["term_id"].astype(int)
    count["count"] = count["count"].astype(int)
    return count


def _load_term_id_map(input_dict, output_dict):
    """loads the term ids from input dict and returns the term_id_map

    Parameters
    ----------
    input_dict : dictionary
        each key is a term partition and each value a LocalTarget
        corresponding to a term_id file
    output_dict : dictionary
        each key is a term partition and each value a LocalTarget
        corresponding to a term_id file

    Returns
    -------
    dictionary of pandas-dfs containing term_ids

    Writes
    ------
    new term_id files
    """

    term_id_offset = 0
    term_map_dict = {}

    for term_part in input_dict:
        with input_dict[term_part].open("r") as fd:
            term_id = pd.read_csv(fd)

        # generate map from old term id to new term id
        term_id_map = term_id[["term_id"]]
        term_id_map = term_id_map.reset_index(drop=True)
        term_id_map["new_term_id"] = term_id_map.index + term_id_offset
        term_id_map.index = term_id_map["term_id"]
        term_id_map = term_id_map["new_term_id"]

        # apply map
        term_id["term_id"] = term_id["term_id"].map(term_id_map)
        with output_dict[term_part].open("w") as fd:
            term_id.to_csv(fd, index=False)
        term_map_dict[term_part] = term_id_map
        term_id_offset += len(term_id)

    return term_map_dict


def _gen_doc_id_length(input_file, tmp_length_file, doc_part):
    """generates the length of each doc id and writes to a temporary file

    Parameters
    ----------
    input_file : LocalTarget
        target corresponding to input doc_id file for current doc_par
    tmp_length_file : str
        location where temporary length file is tored
    doc_part : str
        label for doc partition

    Returns
    -------
    None
    """

    with input_file.open("r") as fd:
        doc_id = pd.read_csv(fd)
    count = len(doc_id)

    with open(tmp_length_file, "a") as fd:
        fd.write("%s,%d\n" % (doc_part, count))


def _reset_doc_part(doc_input_file, doc_output_file,
                    count_input_dict, count_output_dict,
                    doc_partitions, doc_part, tmp_length_file,
                    term_id_map):
    """resets the doc_id for the specified partition and maps the
    new doc_ids to the counts (so reset the indices for an entire doc_part

    Parameters
    ----------
    doc_input_file : LocalTarget
        target corresponding to input doc id for current partition
    doc_output_file : LocalTarget
        target corresponding to output doc id for current partition
    count_input_dict : dict
        dictionary of dicts or LocalTargets corresponding to input count
        files
    count_output_dict : dict
        dictionary of dicts or LocalTargets corresponding to output count
        files
    doc_paritions : list
        list of doc parts, needed to sum all previous lengths
    doc_part : str
        label for current doc_part
    tmp_length_file : str
        location where temporary length file is tored
    term_id_map : dictionary
        dict mapping term_part to term ids which contain term id map

    Returns
    -------
    None

    Writes
    ------
    1. updated doc id
    2. updated counts
    """

    # load temporary lengths
    doc_ind = doc_partitions.index(doc_part)
    agg_length = pd.read_csv(tmp_length_file, names=["part", "length"])
    agg_length.index = agg_length["part"]
    agg_length = agg_length.loc[doc_partitions[:doc_ind]]
    doc_id_offset = agg_length["length"].sum()

    # process doc_id and gen doc_id_map
    with doc_input_file.open("r") as fd:
        doc_id = pd.read_csv(fd)
    doc_id_map = doc_id[["doc_id"]]
    doc_id_map = doc_id_map.reset_index(drop=True)
    doc_id_map["new_doc_id"] = doc_id_map.index + doc_id_offset
    doc_id_map.index = doc_id_map["doc_id"]
    doc_id_map = doc_id_map["new_doc_id"]
    doc_id["doc_id"] = doc_id["doc_id"].map(doc_id_map)
    with doc_output_file.open("w") as fd:
        doc_id.to_csv(fd, index=False)

    # process counts
    for term_part in count_input_dict:
        if count_input_dict[term_part] is dict:
            for count_part in count_input_dict[term_part]:
                count_f = count_input_dict[term_part][count_part]
                with count_f.open("r") as fd:
                    count = pd.read_csv(fd)
                count = _map_count(count, term_id_map[term_part], doc_id_map)
                count_f = count_output_dict[term_part][count_part]
                with count_f.open("w") as fd:
                    count.to_csv(fd, index=False)
        else:
            count_f = count_input_dict[term_part]
            with count_f.open("r") as fd:
                count = pd.read_csv(fd)
            count = _map_count(count, term_id_map[term_part], doc_id_map)
            count_f = count_output_dict[term_part]
            with count_f.open("w") as fd:
                count.to_csv(fd, index=False)



class resetIndexTask(DTMTask):
    """resets the indices along both the doc and term partitions, this is
    comparable to the reset_index method for pandas

    Parameters
    ----------
    tmp_length_file : str
        location where temporary file for doc_id lengths is stored
    """

    tmp_length_file = luigi.Parameter()

    def run(self):

        # load input_dict and output_dict
        input_dict = self.input()
        output_dict = self.output()

        # initialize pool
        pool = multiprocessing.Pool(self.processes)

        # get id lengths for doc_ids
        pool.starmap(_gen_doc_id_length, [(input_dict["doc_id"][doc_part],
                                           self.tmp_length_file, doc_part)
                                           for doc_part in
                                           self.doc_partitions])

        # load term id map and reset term ids
        term_id_map = _load_term_id_map(input_dict["term_id"],
                                        output_dict["term_id"])

        # reset doc ids and apply new doc/term ids to counts
        pool.starmap(_reset_doc_part, [(input_dict["doc_id"][doc_part],
                                        output_dict["doc_id"][doc_part],
                                        input_dict["count"][doc_part],
                                        output_dict["count"][doc_part],
                                        self.doc_partitions, doc_part,
                                        self.tmp_length_file,
                                        term_id_map)
                                       for doc_part in self.doc_partitions])
