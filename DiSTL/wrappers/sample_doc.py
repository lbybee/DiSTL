from DiSTL.post_process.update_doc import update_doc_core


def doc_method(doc_id, counts, doc_part, sample_size, **kwds):
    """samples documents for each partition based on sample_size

    Parameters
    ----------
    doc_id : pd dataframe
        current doc id
    counts : list of pandas dataframes
        current counts
    doc_part : str
        label for current partition
    sample_size : scalar
        number of samples to take from each partition

    Returns
    -------
    updated doc_id and counts
    """

    doc_id = doc_id.sample(min([doc_id.shape[0], sample_size]), replace=False)
    doc_id = doc_id.sort_values("doc_id")

    counts = [c[c["doc_id"].isin(doc_id["doc_id"])] for c in counts]

    return doc_id, counts


def sample_doc_wrapper(in_data_dir, out_data_dir, n_jobs, doc_partitions,
                       term_partitions, sample_size, **kwds):
    """wrapper around update_core to sample documents

    Parameters
    ----------
    in_data_dir : str
        location where input DTM files are located
    out_data_dir : str
        location where output DTM file will be written
    n_jobs : int
        number of jobs for multiprocessing
    doc_partitions : list
        list of partitions over doc axis (e.g. date)
    term_partitions : list
        list of partitions over term axis (e.g. 1gram, 2gram)
    sample_size : scalar
        number of samples to take from each partition

    Returns
    -------
    None

    Writes
    ------
    updated df
    """

    update_method_kwds = {"sample_size": sample_size}

    return update_doc_core(in_data_dir, out_data_dir, n_jobs,
                           doc_partitions, term_partitions, doc_method,
                           update_method_kwds)
