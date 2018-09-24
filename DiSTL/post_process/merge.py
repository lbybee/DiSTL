import dask.dataframe as dd
import pandas as pd
import multiprocessing
import os


def merge_term_partition(in_data_dir, out_data_dir, term_part,
                          doc_partitions, merge_method,
                          merge_methods_kwds,
                          doc_id=None, count_depend=False):
    """merges each partition with the provided methods

    Parameters
    ----------
    in_data_dir : str
        location where input DTM files are located
    out_data_dir : str
        location where output DTM file will be written
    term_part : str
        label for current partition
    doc_partitions : list
        doc partitions
    merge_method : function
        method to apply to each partition
    merge_methods_kwds : dict-like
        key-words to pass to merge method
    doc_id : dd dataframe or None
        doc id if it is needed by merge method
    count_depend : bool
        whether or not the counts are needed by the merge

    Returns
    -------
    None

    Writes
    ------
    merged

    1. main id
    2. count
    """

    # load partition data
    term_f = os.path.join(in_data_dir, "term_id_%s.csv" % term_part)
    term_id = pd.read_csv(term_f)

    count_f_l = [os.path.join(in_data_dir, "count_%s_%s.csv" % (doc_part,
                                                                term_part))
                 for doc_part in doc_partitions]
    counts = dd.read_csv(count_f_l, blocksize=None)

    # apply merge
    if doc_id and count_depend:
        term_id, counts = merge_method(term_id, counts, doc_id,
                                       **merge_method_kwds)
    if doc_id and not count_depend:
        term_id = merge_method(term_id, doc_id, **merge_method_kwds)
    if not doc_id and count_depend:
        term_id, counts = merge_method(term_id, counts, **merge_method_kwds)
    if not doc_id and not count_depend:
        term_id = merge_method(term_id, **merge_method_kwds)

    # write output
    term_f = os.path.join(out_data_dir, "term_id_%s.csv" % term_part)
    term_id.to_csv(term_f, index=False)

    count_f_l = [os.path.join(in_data_dir, "count_%s_%s.csv" % (doc_part,
                                                                term_part))
                 for doc_part in doc_partitions]
    counts.to_csv(count_f_l)


def merge_doc_partition(in_data_dir, out_data_dir, doc_part,
                         term_partitions, merge_method,
                         merge_methods_kwds,
                         term_id=None, count_depend=False):
    """merges each partition with the provided methods

    Parameters
    ----------
    in_data_dir : str
        location where input DTM files are located
    out_data_dir : str
        location where output DTM file will be written
    doc_part : str
        label for current partition
    term_partitions : list
        term partitions
    merge_method : function
        method to apply to each partition
    merge_methods_kwds : dict-like
        key-words to pass to merge method
    term_id : dd dataframe or None
        term id if it is needed by merge method
    count_depend : bool
        whether or not the counts are needed by the merge

    Returns
    -------
    None

    Writes
    ------
    merged

    1. main id
    2. count
    """

    # load partition data
    doc_f = os.path.join(in_data_dir, "doc_id_%s.csv" % doc_part)
    doc_id = pd.read_csv(doc_f)

    count_f_l = [os.path.join(in_data_dir, "count_%s_%s.csv" % (doc_part,
                                                                term_part))
                 for term_part in term_partitions]
    counts = dd.read_csv(count_f_l, blocksize=None)

    # apply merge
    if term_id and count_depend:
        doc_id, counts = merge_method(doc_id, counts, term_id,
                                       **merge_method_kwds)
    if term_id and not count_depend:
        doc_id = merge_method(doc_id, term_id, **merge_method_kwds)
    if not term_id and count_depend:
        doc_id, counts = merge_method(doc_id, counts, **merge_method_kwds)
    if not term_id and not count_depend:
        doc_id = merge_method(doc_id, **merge_method_kwds)

    # write output
    doc_f = os.path.join(out_data_dir, "doc_id_%s.csv" % doc_part)
    doc_id.to_csv(doc_f, index=False)

    count_f_l = [os.path.join(in_data_dir, "count_%s_%s.csv" % (doc_part,
                                                                term_part))
                 for term_part in term_partitions]
    counts.to_csv(count_f_l)


def merge_core(in_data_dir, out_data_dir, merge_data_dir, processes,
               doc_partitions, term_partitions, axis, merge_f_pattern):
    """merge files in the merge_data_dir into the id over the specified
    axis

    Parameters
    ----------
    in_data_dir : str
        location where input DTM files are located
    out_data_dir : str
        location where output DTM file will be written
    merge_data_dir : str
        location of merge files
    processes : int
        number of processes for multiprocessing pool
    doc_partitions : list
        list of partitions over doc axis (e.g. date)
    term_partitions : list
        list of partitions over term axis (e.g. 1gram, 2gram)
    axis : int
        0 for doc axis 1 for term axis
    merge_f_pattern : str
        pattern for merge files should be of form <label>_<part>.csv

    Returns
    -------
    None

    Writes
    ------
    merged

    1. doc_id
    2. term_id
    3. count
    """

    # prep params if needed
    if merge_methods_kwds is None:
        merge_method_kwds = {}

    # init pool
    pool = multiprocess.Pool(processes)

    # determine partitions to loop over
    if axis:
        main_partitions = term_partitions
        alt_partitions = doc_partitions
        merge_partition = merge_term_partition
        alt_pattern = "doc_id_%s.csv"
    else:
        main_partitions = doc_partitions
        alt_partitions = term_partitions
        merge_partition = merge_doc_partition
        alt_pattern = "term_id_%s.csv"

    if alt_depend:
        alt_id = dd.read_csv(os.path.join(in_data_dir, alt_pattern % "*"),
                             block_size=None)
    else:
        alt_id = None

    # merge each main part
    pool.starmap(merge_partition,
                 [(in_data_dir, out_data_dir, main_part, alt_partitions,
                   merge_method, merge_methods_kwds, alt_id,
                   count_depend)
                  for main_part in main_partitions])

    # copy alternative partition files
    pool.starmap(_copy_id,
                 [(alt_pattern % alt_part, in_data_dir, out_data_dir)
                  for alt_part in alt_partitions])
