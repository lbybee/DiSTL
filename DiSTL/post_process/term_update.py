from .utilities import _copy_id
import dask.dataframe as dd
import pandas as pd
import dask
import os


def update_term_partition(in_data_dir, out_data_dir, term_part,
                          doc_partitions, update_method,
                          update_method_kwds,
                          doc_id_dd=None, count_depend=False):
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
    doc_id_dd : dd dataframe or None
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

    update_method_kwds["doc_id_dd"] = doc_id_dd
    update_method_kwds["term_id"] = term_id
    update_method_kwds["term_part"] = term_part
    update_method_kwds["counts"] = counts

    # apply update
    term_id, counts = update_method(**update_method_kwds)

    # write output
    term_f = os.path.join(out_data_dir, "term_id_%s.csv" % term_part)
    term_id.to_csv(term_f, index=False)

    count_f_l = [os.path.join(out_data_dir, "count_%s_%s.csv" % (doc_part,
                                                                 term_part))
                 for doc_part in doc_partitions]
    counts.to_csv(count_f_l, index=False)


def update_term_core(in_data_dir, out_data_dir, doc_partitions,
                     term_partitions, update_method, update_method_kwds=None,
                     doc_id_depend=False, **kwds):
    """applies the provided update method to the partitions along the term
    axis

    Parameters
    ----------
    in_data_dir : str
        location where input DTM files are located
    out_data_dir : str
        location where output DTM file will be written
    doc_partitions : list
        list of partitions over doc axis (e.g. date)
    term_partitions : list
        list of partitions over term axis (e.g. 1gram, 2gram)
    update_method :
        method to apply to each partition
    update_method_kwds : list or None
        key-words to pass to update method
    term_id_depend : bool
        whether the update method depends on the doc_id

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
    The main difference with update_doc_core is that we asssume the
    parallelization should be over the alternative (doc) axis instead of
    the main (term) axis
    """

    # prep params if needed
    if update_method_kwds is None:
        update_method_kwds = {}

    if doc_id_depend:
        doc_id_dd = dd.read_csv(os.path.join(in_data_dir, "doc_id_*.csv"),
                                blocksize=None, assume_missing=True)
        doc_id_dd = dask.persist(doc_id_dd)[0]
    else:
        doc_id_dd = None

    for term_part in term_partitions:
        update_term_partition(in_data_dir, out_data_dir, term_part,
                              doc_partitions, update_method,
                              update_method_kwds, doc_id_dd)

    for doc_part in doc_partitions:
        _copy_id("doc_id_%s.csv" % doc_part, in_data_dir, out_data_dir)
