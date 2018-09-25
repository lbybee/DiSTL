from dask import delayed
import dask.dataframe as dd
import multiprocessing
import os


def _del_sort(x):
    return x.sort_values("doc_id")


def prep_counts(in_f_pattern, out_f_pattern):
    """generates a single file containing counts which can be passsed into
    julia

    Parameters
    ----------
    in_f_pattern : str or list
        pattern which can be read by dask dataframe read_csv
    out_f_pattern : str
        location where resulting combined df should be written

    Returns
    -------
    None

    Writes
    ------
    counts csv
    """

    counts = dd.read_csv(in_f_pattern, blocksize=None)
    del_counts = counts.to_delayed()
    del_counts = [delayed(_del_sort)(c) for c in del_counts]
    counts = dd.from_delayed(del_counts)
    counts["term_id"] = counts["term_id"] + 1
    counts = counts.compute()
    counts["doc_id"] = counts["doc_id"].rank(method="dense").astype(int)
    counts.to_csv(out_f_pattern, index=False)


def prep_covars(in_f_pattern, out_f_pattern, read_csv_kwds=None,
                rename_dict=None):
    """generates a single file containing covars which can be passsed into
    julia

    Parameters
    ----------
    in_f_pattern : str or list
        pattern which can be read by dask dataframe read_csv
    out_f_pattern : str
        location where resulting combined df should be written
    read_csv_kwds : dict-like or None
        key-words passed to dd read csv
    rename_dict : dict-like or None
        if dict-like then we use this to rename columns and the resulting
        set of covars will be the values

    Returns
    -------
    None

    Writes
    ------
    covars csv
    """

    if read_csv_kwds is None:
        read_csv_kwds = {}

    covars = dd.read_csv(in_f_pattern, **read_csv_kwds)
    del_covars = covars.to_delayed()
    del_covars = [delayed(_del_sort)(c) for c in del_covars]
    covars = dd.from_delayed(del_covars)
    covars = covars.compute()
    if rename_dict:
        covars = covars.rename(columns=rename_dict)
        covars = covars[[v for v in rename_dict.values()]]
    covars.to_csv(out_f_pattern, index=False)


def part_builder(tt_part_id, train_part, test_part, term_partitions,
                 in_data_dir, out_data_dir, read_csv_kwds,
                 rename_dict):
    """builds the training test files for each tt part

    Parameters
    ----------
    tt_part_id : int
        id for current tt part
    train_part : list
        list of training partitions (from doc_partitions)
    test_part : list
        list of test partitions (from doc_parititons)
    term_parttions : list
        list of term paritions
    in_data_dir : str
        input dir
    out_data_dir : str
        location of output files
    read_csv_kwds : dict
        key-words to pass to dd reader
    rename_dict : dict
        names (see tt part wrapper)

    REturns
    -------
    None

    Writes
    ------
    training/test partitions
    """

    # train counts
    prep_counts([os.path.join(data_dir, "count_%s_%s.csv" % (n, d))
                 for d in train_part for n in ngrams],
                os.path.join(julia_dir, "%d_train_counts.csv" % tt_part_id))

    # test counts
    prep_counts([os.path.join(data_dir, "count_%s_%s.csv" % (n, d))
                 for d in test_part for n in ngrams],
                os.path.join(julia_dir, "%d_test_counts.csv" % tt_part_id))

    # train covars
    prep_covars([os.path.join(data_dir, "doc_id_%s.csv" % d)
                 for d in train_part],
                os.path.join(julia_dir, "%d_train_covars.csv" % tt_part_id),
                read_csv_kwds=read_csv_kwds,
                rename_dict=rename_dict)

    # test covars
    prep_covars([os.path.join(data_dir, "doc_id_%s.csv" % d)
                 for d in test_part],
                os.path.join(julia_dir, "%d_test_covars.csv" % tt_part_id),
                read_csv_kwds=read_csv_kwds,
                rename_dict=rename_dict)


def tt_part_wrapper(doc_partitions, term_partitions, in_data_dir,
                    out_data_dir, rename_dict, processes, tt_part_count,
                    read_csv_kwds):
    """wrapper to split the data into a series of training and test
    partitions

    Parameters
    ----------
    doc_partitions : list
        list of doc parts
    term_partitions : list
        list of term parts
    in_data_dir : str
        location of input files
    out_data_dir : str
        location of output files
    rename_dict : dict
        dictionary mapping current columsn to new columns, the values are
        used as the covars to keep
    processes : int
        number of processes for multiprocessing pool
    tt_part_count : int
        number of training/test partitions
    read_csv_kwds : dict or None
        key-words to pass to covar dd csv reader

    Returns
    -------
    None

    Writes
    ------
    collapses training test partition files
    """

    # init pool
    pool = multiprocessing.Pool(processes)

    # prep partitions
    doc_partitions.sort()
    stp_size = int(len(doc_partitions) / tt_part_count)
    testing_partitions = []
    training_partitions = []
    for k in range(1, tt_part_count - 1):
        tmp = doc_partitions[(k * stp_size):((k + 1) * stp_size)]
        test_partitions.append(tmp)
        training_partitions.append(doc_partitions[:(k * stp_size)] +
                                   doc_partitions[((k + 1) * stp_size):])
    testing_partitions.append(doc_partitions[(tt_part_count * stp_size):])
    training_partitions.append(doc_partitions[:(tt_part_count * stp_size)])

    # build partitions
    pool.starmap(part_builder,
                 [(k, part[0], part[1], term_partitions, in_data_dir,
                   out_data_dir, read_csv_kwds, rename_dict)
                  for k, part in enumerate(zip(train_partitions,
                                               test_partitions))])
