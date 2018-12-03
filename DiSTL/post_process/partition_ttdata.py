from dask import delayed
import dask.dataframe as dd
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
    covars = covars.compute()
    covars = covars.sort_values("doc_id")
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
    prep_counts([os.path.join(in_data_dir, "count_%s_%s.csv" % (d, n))
                 for d in train_part for n in term_partitions],
                os.path.join(out_data_dir,
                             "%d_train_counts.csv" % tt_part_id))

    # test counts
    prep_counts([os.path.join(in_data_dir, "count_%s_%s.csv" % (d, n))
                 for d in test_part for n in term_partitions],
                os.path.join(out_data_dir,
                             "%d_test_counts.csv" % tt_part_id))

    # train covars
    prep_covars([os.path.join(in_data_dir, "doc_id_%s.csv" % d)
                 for d in train_part],
                os.path.join(out_data_dir,
                             "%d_train_covars.csv" % tt_part_id),
                read_csv_kwds=read_csv_kwds,
                rename_dict=rename_dict)

    # test covars
    prep_covars([os.path.join(in_data_dir, "doc_id_%s.csv" % d)
                 for d in test_part],
                os.path.join(out_data_dir,
                             "%d_test_covars.csv" % tt_part_id),
                read_csv_kwds=read_csv_kwds,
                rename_dict=rename_dict)


def gen_multi_tt_part(doc_partitions, term_partitions, in_data_dir,
                      out_data_dir, rename_dict, read_csv_kwds,
                      tt_part_count):
    """generates a series of training/test partitions from the DTM based
    on the tt_part_count

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
    read_csv_kwds : dict or None
        key-words to pass to covar dd csv reader
    tt_part_count : int
        number of training/test partitions

    Returns
    -------
    None

    Writes
    ------
    collapses training test partition files
    """

    # prep partitions
    doc_partitions.sort()
    stp_size = int(len(doc_partitions) / tt_part_count)
    test_partitions = []
    train_partitions = []
    for k in range(tt_part_count - 1):
        tmp = doc_partitions[(k * stp_size):((k + 1) * stp_size)]
        test_partitions.append(tmp)
        train_partitions.append(doc_partitions[:(k * stp_size)] +
                                doc_partitions[((k + 1) * stp_size):])
    test_partitions.append(doc_partitions[(tt_part_count * stp_size):])
    train_partitions.append(doc_partitions[:(tt_part_count * stp_size)])

    # build partitions
    for k, part in enumerate(zip(train_partitions, test_partitions)):
        train_part, test_part = part
        part_builder(k, train_part, test_part, term_partitions, in_data_dir,
                     out_data_dir, read_csv_kwds, rename_dict)


def gen_single_tt_part(doc_partitions, term_partitions, in_data_dir,
                       out_data_dir, rename_dict, read_csv_kwds,
                       tt_test_part_prop, tt_part_loc):
    """generates a single training/test partition from the DTM based on
    the tt_part_prop and tt_part_loc

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
    read_csv_kwds : dict or None
        key-words to pass to covar dd csv reader
    tt_test_part_prop : float
        proportion of sample to include in test vs training partition
    tt_test_part_loc : str
        where to put test partition:

        1. start
        2. end
        3. middle

    Returns
    -------
    None

    Writes
    ------
    collapses training test partition files
    """

    # prep partitions
    doc_partitions.sort()
    Dp = len(doc_partitions)
    test_part_size = int(tt_test_part_prop * Dp)


    if tt_part_loc == "start":
        train_partitions = doc_partitions[test_part_size:]
        test_partitions = doc_partitions[:test_part_size]
    elif tt_part_loc == "end":
        train_partitions = doc_partitions[:(Dp - test_part_size)]
        test_partitions = doc_partitions[(Dp - test_part_size):]
    elif tt_part_loc == "middle":
        mid_low = int((Dp - test_part_size) / 2.)
        mid_high = int((Dp + test_part_size) / 2.)
        train_partitions = (doc_partitions[:mid_low] +
                            doc_partitions[mid_high:])
        test_partitions = doc_partitions[mid_low:mid_high]
    else:
        raise ValueError("tt_part_loc: %s unsupported")

    part_builder(0, train_partitions, test_partitions, term_partitions,
                 in_data_dir, out_data_dir, read_csv_kwds, rename_dict)


def tt_part_wrapper(doc_partitions, term_partitions, in_data_dir,
                    out_data_dir, rename_dict, read_csv_kwds, tt_part_count,
                    tt_test_part_prop=None, tt_part_loc=None, **kwds):
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
    read_csv_kwds : dict or None
        key-words to pass to covar dd csv reader
    tt_part_count : int
        number of training/test partitions

    Returns
    -------
    None

    Writes
    ------
    collapses training test partition files
    """

    if tt_part_count > 1:
        gen_multi_tt_part(doc_partitions, term_partitions, in_data_dir,
                          out_data_dir, rename_dict, read_csv_kwds,
                          tt_part_count)

    elif tt_part_count == 1:
        if tt_test_part_prop is None or tt_part_loc is None:
            raise ValueError("tt_test_part_prop and tt_part_loc can't be " +
                             "None if tt_part_count == 1")
        else:
            gen_single_tt_part(doc_partitions, term_partitions, in_data_dir,
                               out_data_dir, rename_dict, read_csv_kwds,
                               tt_test_part_prop, tt_part_loc)

    else:
        raise ValueError("unsupported tt_part_count: %d" % tt_part_count)
