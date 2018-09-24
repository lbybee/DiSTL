import multiprocessing
import os


def DTM_checker(doc_partitions, term_partitions, count_partitions,
                processes, out_data_dir, **kwds):
    """a method for checking whether a series of files corresponding
    to an updated DTM exist

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
    out_data_dir : str
        location where output files will be stored

    Returns
    -------
    True if all files exist, false if not
    """



    # init multiprocessing pool
    pool = multiprocessing.Pool(processes)

    # check doc_id files
    doc_paths = [os.path.join(out_data_dir, "doc_id_%s.csv" % doc_part)
                 for doc_part in doc_partitions]
    doc_state = sum(pool.map(os.path.exists, doc_paths))

    # check term_id files
    term_paths = [os.path.join(out_data_dir, "term_id_%s.csv" % term_part)
                  for term_part in term_partitions]
    term_state = sum(pool.map(os.path.exists, term_paths))

    # check count files
    count_paths = [os.path.join(out_data_dir,
                                "count_%s_%s_%s.csv" % (doc_part,
                                                        term_part,
                                                        count_part))
                   for doc_part in doc_partitions
                   for term_part in term_partitions
                   for count_part in count_partitions]
    count_state = sum(pool.map(os.path.exists, count_paths))

    return doc_state and term_state and count_state


def DTM_runner(method, method_kwds):
    """a wrapper for running DTM methods, this only runs the necessary code
    if the resulting files don't already exist

    Parameters
    ----------
    method : function
        what is called if files don't exist
    method_kwds : dict-like
        key-words to pass to method

    Returns
    -------
    out_data_dir location
    """

    out_data_dir = method_kwds["out_data_dir"]
    if not os.path.exists(out_data_dir):
        os.makedirs(out_data_dir, exist_ok=True)

    if DTM_checker(**method_kwds):
        return out_data_dir

    else:
        method(**method_kwds)
        return out_data_dir
