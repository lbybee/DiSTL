from .utilities import _copy_id
import dask.dataframe as dd
import multiprocessing
import luigi
import os


def _sum_counts(doc_part, term_part, in_data_dir, out_data_dir):
    """take multiple count partitions and sum into one count file

    Parameters
    ----------
    doc_part : str
        label for current doc_part
    term_part : str
        label for current term part
    in_data_dir : str
        location of input files
    out_data_dir : str
        location of output files

    Returns
    -------
    None

    Writes
    ------
    updated count file
    """

    count_f = os.path.join(in_data_dir, "count_%s_%s_*.csv" % (doc_part,
                                                               term_part))
    count = dd.read_csv(count_f)
    count = count.groupby(["doc_id", "term_id"])["count"].sum().compute()
    count = count.reset_index()

    count_f = os.path.join(out_data_dir, "count_%s_%s.csv" % (doc_part,
                                                              term_part))
    count.to_csv(count_f, index=False)


def collapse_wrapper(doc_partitions, term_partitions, count_partitions,
                     processes, in_data_dir, out_data_dir, **kwds):
    """aggregate a DTM over the count partitions

    Parameters
    ----------
    doc_partitions : list
        list of partition labels for doc axis (e.g. dates)
    term_partitions : list
        list of partition labels for term axis (e.g. 1gram, 2gram)
    count_partitions : list
        list of partition labels for different count types (e.g. headline,
        body)
    processes : int
        number of processes for multiprocessing pool
    in_data_dir : str
        location where input files are stored
    out_data_dir : str
        location where output files will be stored

    Returns
    -------
    None

    Writes
    ------
    1. copy of doc_id
    2. copy of term_id
    3. updated counts (collapsed over count_parts)
    """


    if len(count_partitions) == 0:
        raise ValueError("Can't collapse count partitions if none exist")

    # initialize pool
    pool = multiprocessing.Pool(processes)

    # sum the count file for each doc_part and term_part over
    # the count_partitions
    pool.starmap(_sum_counts, [(doc_part, term_part, in_data_dir,
                                out_data_dir)
                               for doc_part in doc_partitions
                               for term_part in term_partitions])

    # copy doc_ids and term_ids
    pool.starmap(_copy_id, [("doc_id_%s.csv" % doc_part, in_data_dir,
                             out_data_dir) for doc_part in doc_partitions])
    pool.starmap(_copy_id, [("term_id_%s.csv" % term_part, in_data_dir,
                             out_data_dir) for term_part in term_partitions])

    # close the pool
    pool.close()
