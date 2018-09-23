import pandas as pd


def _process_base_partition(part_input_file, count_input_dict,
                            part_output_file, count_output_dict,
                            update_method, update_method_kwds, ax_label):
    """update part_id without conditioning on any other info

    Parameters
    ----------
    part_input_file : LocalTarget
        input id file object
    count_input_dict : dictionary
        dictionary mapping to input LocalTarget for each count file for
        current part
    part_output_file : LocalTarget
        output id file object
    count_output_dict : LocalTarget
        dictionary mapping to output LocalTarget for each count file for
        current part
    update_method : function
        method for updating part_id
    update_method_kwds : dict-like
        key-words passed to update_method
    ax_label : str
        label for axis over which we are partitioning (doc_id or term_id)

    Returns
    -------
    None

    Writes
    ------
    updated part_id and counts
    """

    # load part_id
    with part_input_file.open("r") as fd:
        part_id = pd.read_csv(fd)

    # update part_id
    part_id = update_method(part_id, **update_method_kwds)

    # write new part_id
    with part_output_file.open("w") as fd:
        part_id.to_csv(fd, index=False)

    # update counts
    for alt_part in count_input_dict:
        count_f = count_input_dict[alt_part]
        with count_f.open("r") as fd:
            count = pd.read_csv(fd)
        count = count[count[ax_label].isin(part_id[ax_label])]
        count_f = count_output_dict[alt_part]
        with count_f.open("w") as fd:
            count.to_csv(fd, index=False)


def _process_metadata_partition(part_input_file, count_input_dict,
                                metadata_input_file, part_output_file,
                                count_output_dict, update_method,
                                update_method_kwds, ax_label):
    """merge metadata with part_id

    Parameters
    ----------
    part_input_file : LocalTarget
        input id file object
    count_input_dict : dictionary
        dictionary mapping to input LocalTarget for each count file for
        current part
    metadata_input_file : LocalTarget
        input metdata file object
    part_output_file : LocalTarget
        output id file object
    count_output_dict : LocalTarget
        dictionary mapping to output LocalTarget for each count file for
        current part
    update_method : function
        method for updating part_id
    update_method_kwds : dict-like
        key-words passed to update_method
    ax_label : str
        label for axis over which we are partitioning (doc_id or term_id)

    Returns
    -------
    None

    Writes
    ------
    updated part_id and counts
    """

    # load part_id and metadata
    with part_input_file.open("r") as fd:
        part_id = pd.read_csv(fd)

    with metadata_input_file.open("r") as fd:
        metadata_df = pd.read_csv(fd)

    # update part_id
    part_id = update_method(part_id, metadata_df, **update_method_kwds)

    # write new part_id
    with part_output_file.open("w") as fd:
        part_id.to_csv(fd, index=False)

    # update counts
    for alt_part in count_input_dict:
        count_f = count_input_dict[alt_part]
        with count_f.open("r") as fd:
            count = pd.read_csv(fd)
        count = count[count[ax_label].isin(part_id[ax_label])]
        count_f = count_output_dict[alt_part]
        with count_f.open("w") as fd:
            count.to_csv(fd, index=False)


def _process_count_partition(part_input_file, count_input_dict,
                             alt_id_input_dict, part_output_file,
                             count_output_dict, update_method,
                             update_method_kwds, ax_label):
    """update part_id using counts and potentially alt_ids

    Parameters
    ----------
    part_input_file : LocalTarget
        input id file object
    count_input_dict : dictionary
        dictionary mapping to input LocalTarget for each count file for
        current part
    alt_id_input_dict : dictionary
        dictionary mapping to input files for alternative axis
    part_output_file : LocalTarget
        output id file object
    count_output_dict : LocalTarget
        dictionary mapping to output LocalTarget for each count file for
        current part
    update_method : function
        method for updating part_id
    update_method_kwds : dict-like
        key-words passed to update_method
    ax_label : str
        label for axis over which we are partitioning (doc_id or term_id)

    Returns
    -------
    None

    Writes
    ------
    updated part_id and counts
    """

    # load part_id and metadata
    with part_input_file.open("r") as fd:
        part_id = pd.read_csv(fd)

    # update part_id
    part_id = update_method(part_id, count_input_dict, alt_id_input_dict,
                            **update_method_kwds)

    # write new part_id
    with part_output_file.open("w") as fd:
        part_id.to_csv(fd, index=False)

    # update counts
    for alt_part in count_input_dict:
        count_f = count_input_dict[alt_part]
        with count_f.open("r") as fd:
            count = pd.read_csv(fd)
        count = count[count[ax_label].isin(part_id[ax_label])]
        count_f = count_output_dict[alt_part]
        with count_f.open("w") as fd:
            count.to_csv(fd, index=False)
