from .utilities import _copy_id
import dask.dataframe as dd
import pandas as pd
import multiprocessing
import dask
import os


def update_term_partition(in_data_dir, out_data_dir, term_part,
                          doc_partitions, update_method,
                          update_method_kwds,
                          doc_id=None, count_depend=False):
    """updates each partition with the provided methods

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
    update_method : function
        method to apply to each partition
    update_methods_kwds : dict-like
        key-words to pass to update method
    doc_id : dd dataframe or None
        doc id if it is needed by update method
    count_depend : bool
        whether or not the counts are needed by the update

    Returns
    -------
    None

    Writes
    ------
    updated

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

    update_method_kwds["doc_id"] = doc_id
    update_method_kwds["term_id"] = term_id
    update_method_kwds["term_part"] = term_part
    update_method_kwds["counts"] = counts

    # apply update
    term_id, counts = update_method(**update_method_kwds)

    # write output
    term_f = os.path.join(out_data_dir, "term_id_%s.csv" % term_part)
    term_id.to_csv(term_f, index=False)

    count_f_l = [os.path.join(in_data_dir, "count_%s_%s.csv" % (doc_part,
                                                                term_part))
                 for doc_part in doc_partitions]
    counts.to_csv(count_f_l)


def update_doc_partition(in_data_dir, out_data_dir, doc_part,
                         term_partitions, update_method,
                         update_method_kwds,
                         term_id=None):
    """updates each partition with the provided methods

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
    update_method : function
        method to apply to each partition
    update_methods_kwds : dict-like
        key-words to pass to update method
    term_id : dd dataframe or None
        term id if it is needed by update method

    Returns
    -------
    None

    Writes
    ------
    updated

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

    update_method_kwds["doc_id"] = doc_id
    update_method_kwds["term_id"] = term_id
    update_method_kwds["doc_part"] = doc_part
    update_method_kwds["counts"] = counts

    # apply update
    doc_id, counts = update_method(**update_method_kwds)

    # write output
    doc_f = os.path.join(out_data_dir, "doc_id_%s.csv" % doc_part)
    doc_id.to_csv(doc_f, index=False)

    count_f_l = [os.path.join(out_data_dir, "count_%s_%s.csv" % (doc_part,
                                                                 term_part))
                 for term_part in term_partitions]
    counts.to_csv(count_f_l)


def update_core(in_data_dir, out_data_dir, processes, doc_partitions,
                term_partitions, axis, update_method,
                update_method_kwds=None, alt_id_depend=False, **kwds):
    """applies the provided update method to the partitions along the given
    axis

    Parameters
    ----------
    in_data_dir : str
        location where input DTM files are located
    out_data_dir : str
        location where output DTM file will be written
    processes : int
        number of processes for multiprocessing pool
    doc_partitions : list
        list of partitions over doc axis (e.g. date)
    term_partitions : list
        list of partitions over term axis (e.g. 1gram, 2gram)
    axis : int
        0 for doc axis 1 for term axis
    update_method :
        method to apply to each partition
    update_method_kwds : list or None
        key-words to pass to update method
    alt_id_depend : bool
        whether the update method depends on the alt id

    Returns
    -------
    None

    Writes
    ------
    updated

    1. doc_id
    2. term_id
    3. count
    """

    # prep params if needed
    if update_method_kwds is None:
        update_method_kwds = {}

    # init pool
    pool = multiprocessing.Pool(processes)

    # determine partitions to loop over
    if axis:
        main_partitions = term_partitions
        alt_partitions = doc_partitions
        update_partition = update_term_partition
        alt_pattern = "doc_id_%s.csv"
    else:
        main_partitions = doc_partitions
        alt_partitions = term_partitions
        update_partition = update_doc_partition
        alt_pattern = "term_id_%s.csv"

    if alt_id_depend:
        alt_id = dd.read_csv(os.path.join(in_data_dir, alt_pattern % "*"),
                             blocksize=None, assume_missing=True)
        alt_id = dask.persist(alt_id)[0]
    else:
        alt_id = None

    # update each main part
    pool.starmap(update_partition,
                 [(in_data_dir, out_data_dir, main_part, alt_partitions,
                   update_method, update_method_kwds, alt_id)
                  for main_part in main_partitions])

    # copy alternative partition files
    pool.starmap(_copy_id,
                 [(alt_pattern % alt_part, in_data_dir, out_data_dir)
                  for alt_part in alt_partitions])
