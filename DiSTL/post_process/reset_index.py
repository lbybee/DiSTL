import dask.dataframe as dd
import pandas as pd
import multiprocessing
import os


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


def _load_term_id_map(term_partitions, in_data_dir, out_data_dir,
                      u_term_id):
    """loads the term ids from input dict and returns the term_id_map

    Parameters
    ----------
    term_partitions : list
        list of term partitions
    in_data_dir : str
        location of input files
    out_data_dir : str
        location of output files
    u_term_id : pd series
        series of unique term ids which still have counts

    Returns
    -------
    dictionary of pandas-dfs containing term_ids

    Writes
    ------
    new term_id files
    """

    term_id_offset = 0
    term_map_dict = {}

    for term_part in term_partitions:
        term_f = os.path.join(in_data_dir, "term_id_%s.csv" % term_part)
        term_id = pd.read_csv(term_f)

        # constrain to count term_ids
        term_id = term_id[term_id["term_id"].isin(u_term_id)]

        # generate map from old term id to new term id
        term_id_map = term_id[["term_id"]]
        term_id_map = term_id_map.reset_index(drop=True)
        term_id_map["new_term_id"] = term_id_map.index + term_id_offset
        term_id_map.index = term_id_map["term_id"]
        term_id_map = term_id_map["new_term_id"]

        # apply map
        term_id["term_id"] = term_id["term_id"].map(term_id_map)
        term_f = os.path.join(out_data_dir, "term_id_%s.csv" % term_part)
        term_id.to_csv(term_f, index=False)
        term_map_dict[term_part] = term_id_map
        term_id_offset += len(term_id)

    return term_map_dict


def _gen_doc_id_length(doc_part, in_data_dir, tmp_length_file, u_doc_id):
    """generates the length of each doc id and writes to a temporary file

    Parameters
    ----------
    doc_part : str
        label for doc partition
    in_data_dir : str
        location of input files
    tmp_length_file : str
        location where temporary length file is tored
    u_doc_id : pd series
        unique doc ids which have a count

    Returns
    -------
    None
    """

    doc_f = os.path.join(in_data_dir, "doc_id_%s.csv" % doc_part)
    doc_id = pd.read_csv(doc_f)

    # constrain to count doc_ids
    doc_id = doc_id[doc_id["doc_id"].isin(u_doc_id)]

    count = len(doc_id)

    with open(tmp_length_file, "a") as fd:
        fd.write("%s,%d\n" % (doc_part, count))


def _reset_doc_part(doc_part, doc_partitions, term_partitions,
                    count_partitions, in_data_dir, out_data_dir,
                    tmp_length_file, u_doc_id, term_id_map):
    """resets the doc_id for the specified partition and maps the
    new doc_ids to the counts (so reset the indices for an entire doc_part

    Parameters
    ----------
    doc_part : str
        label for currrent doc partition
    doc_partitions : list
        list of doc parts (needed to get offset)
    term_partitions : list
        list of term parts
    count_partitions : list
        list of count partitions
    in_data_dir : str
        location of input files
    out_data_dir : str
        location of output files
    tmp_length_file : str
        location where temporary length file is tored
    u_doc_id : pd series
        unique doc ids which have a count
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

    # process doc_id
    doc_f = os.path.join(in_data_dir, "doc_id_%s.csv" % doc_part)
    doc_id = pd.read_csv(doc_f)
    # constrain to count doc_ids
    doc_id = doc_id[doc_id["doc_id"].isin(u_doc_id)]
    # gen doc_id_map
    doc_id_map = doc_id[["doc_id"]]
    doc_id_map = doc_id_map.reset_index(drop=True)
    doc_id_map["new_doc_id"] = doc_id_map.index + doc_id_offset
    doc_id_map.index = doc_id_map["doc_id"]
    doc_id_map = doc_id_map["new_doc_id"]
    doc_id["doc_id"] = doc_id["doc_id"].map(doc_id_map)
    doc_f = os.path.join(out_data_dir, "doc_id_%s.csv" % doc_part)
    doc_id.to_csv(doc_f, index=False)

    # process counts
    for term_part in term_partitions:
        if len(count_partitions) > 0:
            for count_part in count_partitions:
                count_f = os.path.join(in_data_dir, "count_%s_%s_%s.csv" %
                                       (doc_part, term_part, count_part))
                count = pd.read_csv(count_f)
                count = _map_count(count, term_id_map[term_part], doc_id_map)
                count_f = os.path.join(out_data_dir, "count_%s_%s_%s.csv" %
                                       (doc_part, term_part, count_part))
                count.to_csv(count_f, index=False)
        else:
            count_f = os.path.join(in_data_dir, "count_%s_%s.csv" %
                                   (doc_part, term_part))
            count = pd.read_csv(count_f)
            count = _map_count(count, term_id_map[term_part], doc_id_map)
            count_f = os.path.join(out_data_dir, "count_%s_%s.csv" %
                                   (doc_part, term_part))
            count.to_csv(count_f, index=False)


def reset_index_wrapper(doc_partitions, term_partitions, tmp_length_file,
                        processes, in_data_dir, out_data_dir,
                        count_partitions=None, **kwds):
    """reset the indices of a DTM that may have been manipulated by other
    operations

    Parameters
    ----------
    doc_partitions : list
        list of partition labels for doc axis (e.g. dates)
    term_partitions : list
        list of partition labels for term axis (e.g. 1gram, 2gram)
    tmp_length_file : str
        location of intermediate file where lengths are stored for doc
        parts
    processes : int
        number of processes for multiprocessing pool
    in_data_dir : str
        location where input files are stored
    out_data_dir : str
        location where output files will be stored
    count_partitions : list or None
        list of partition labels for different count types (e.g. headline,
        body)

    Returns
    -------
    None

    Writes
    ------
    1. updated doc_id
    2. updated term_id
    3. updated counts
    """

    if count_partitions is None:
        count_partitions = []

    # prep lists of remaining doc_ids and term_ids (this is because some
    # counts may have dropped eliminating certain docs/terms
    counts = dd.read_csv(os.path.join(in_data_dir, "count_*.csv"),
                         blocksize=None)
    u_doc_id = counts["doc_id"].drop_duplicates().compute()
    u_term_id = counts["term_id"].drop_duplicates().compute()

    # initialize pool
    pool = multiprocessing.Pool(processes)

    # get id lengths for doc_ids
    pool.starmap(_gen_doc_id_length, [(doc_part, in_data_dir,
                                       tmp_length_file, u_doc_id)
                                      for doc_part in doc_partitions])

    # load term id map and reset term ids
    term_id_map = _load_term_id_map(term_partitions, in_data_dir,
                                    out_data_dir, u_term_id)

    # reset doc ids and apply new doc/term ids to counts
    pool.starmap(_reset_doc_part,
                 [(doc_part, doc_partitions, term_partitions,
                   count_partitions, in_data_dir, out_data_dir,
                   tmp_length_file, u_doc_id, term_id_map)
                  for doc_part in doc_partitions])

    # close pool
    pool.close()
