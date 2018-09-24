import multiprocessing

def copy_partition(in_data_dir, out_data_dir, alt_part):
    """copies the files for the alt partition

    Parameters
    ----------
    in_data_dir : str
        location where input DTM files are located
    out_data_dir : str
        location where output DTM file will be written
    alt_part : str
        label for current partition

    Returns
    -------
    None

    Writes
    ------
    updated alt id
    """

    return None


def update_doc_partition(in_data_dir, out_data_dir, doc_part,
                         term_partitions, update_methods_l,
                         update_methods_kwds_l,
                         term_id=None):
    """updates each partition with the provided methods

    Parameters
    ----------
    in_data_dir : str
        location where input DTM files are located
    out_data_dir : str
        location where output DTM file will be written
    main_part : str
        label for current partition
    alt_partitions : list
        list of partitions over alternative axis
    update_methods_l : list
        list of methods to apply to each partition
    update_methods_kwds_l : list
        key-words to pass to each update method

    Returns
    -------
    None

    Writes
    ------
    updated

    1. main id
    2. count
    """

    return None



def update_DTM(in_data_dir, out_data_dir, processes, doc_partitions,
               term_partitions, axis, update_methods_l,
               update_methods_kwds_l=None, count_partitions=None):
    """applies a series of methods to the partitions along the given axis

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
    update_methods_l : list
        list of methods to apply to each partition
    update_methods_kwds_l : list or None
        key-words to pass to each update method
    count_partitions : list or None
        if None, this is a series of partitions over count files
        (e.g. body, headline)

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
    if update_methods_kwds_l is None:
        update_methods_kwds_l = [{} for method in update_methods_l]

    # init pool
    pool = multiprocess.Pool(processes)

    # determine partitions to loop over
    if axis:
        main_partitions = term_partitions
        alt_partitions = doc_partitions
    else:
        main_partitions = doc_partitions
        alt_partitions = term_partitions

    # TODO add count partitions
    # update each main part
    pool.starmap(update_partition,
                 [(in_data_dir, out_data_dir, main_part, alt_partitions,
                   update_methods_l, update_methods_kwds_l)
                  for main_part in main_partitions])

    # copy alternative partition files
    pool.starmap(copy_alt_part_files,
                 [(in_data_dir, out_data_dir, axis, alt_part)
                  for alt_part in alt_partitions])
