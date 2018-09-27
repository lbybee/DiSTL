from .utilities import _copy_id
import multiprocessing
import pandas as pd
import os


def update_doc_partition(in_data_dir, out_data_dir, doc_part,
                         term_partitions, update_method,
                         update_method_kwds, term_id_l=None):
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
    term_id_l : list of pandas dataframes or None
        term_ids if it is needed by update method

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
    counts = [pd.read_csv(count_f) for count_f in count_f_l]

    update_method_kwds["doc_id"] = doc_id
    update_method_kwds["term_id_l"] = term_id_l
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
    [count_p.to_csv(count_f, index=False) for count_p, count_f
     in zip(counts, count_f_l)]


def update_doc_core(in_data_dir, out_data_dir, processes, doc_partitions,
                    term_partitions, update_method, update_method_kwds=None,
                    term_id_depend=False, **kwds):
    """applies the provided update method to the partitions along the doc
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
    update_method :
        method to apply to each partition
    update_method_kwds : list or None
        key-words to pass to update method
    term_id_depend : bool
        whether the update method depends on the term_id

    Returns
    -------
    None

    Writes
    ------
    updated

    1. doc_id
    2. term_id
    3. count

    Notes
    -----
    The main difference with update_term_core is that we asssume the
    parallelization should be over the main (doc) axis instead of the
    alternative (term) axis
    """

    # prep params if needed
    if update_method_kwds is None:
        update_method_kwds = {}

    if term_id_depend:
        term_f_l = [os.path.join(in_data_dir, "term_id_%s.csv" % term_part)
                    for term_part in term_partitions]
        term_id_l = [pd.read_csv(term_f) for term_f in term_f_l]
    else:
        term_id_l = None

    # init pool
    pool = multiprocessing.Pool(processes)

    # update each doc part
    pool.starmap(update_doc_partition,
                 [(in_data_dir, out_data_dir, doc_part, term_partitions,
                   update_method, update_method_kwds, term_id_l)
                  for doc_part in doc_partitions])

    # copy term partition files
    pool.starmap(_copy_id,
                 [("term_id_%s.csv" % term_part, in_data_dir, out_data_dir)
                  for term_part in term_partitions])

    # close pool
    pool.close()
