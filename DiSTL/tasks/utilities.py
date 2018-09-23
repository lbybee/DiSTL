"""
helper functions used by multiple tasks
"""

def _copy_id(input_file, output_file):
    """copies the ids from their input file to output file (since they aren't
    touched when we collapse

    Parameters
    ----------
    input_file : LocalTarget
        input id file object
    output_file : LocalTarget
        output id file object

    Returns
    -------
    None

    Writes
    ------
    id files in new location
    """

    with input_file.open("r") as in_fd:
        with output_file.open("w") as out_fd:
            out_fd.write(in_fd.read())



def _process_counts(count_input_dict, count_output_dict, update_method,
                    update_method_kwds=None):
    """reads count files updates and writes

    Parameters
    ----------
    count_input_dict : dictionary
        mapping to input LocalTarget objects
    count_output_dict : dictionary
        mapping to output LocalTarget objects
    update_method : function
        method applied to counts when loaded
    update_method_kwds : dict-like or None
        key-words passed to update method

    Returns
    -------
    None

    Writes
    ------
    updated counts
    """

    raise NotImplementedError("_process_counts not ready")

    if update_method_kwds is None:
        update_method_kwds = {}

    for alt_part in count_input_dict:
        if count_input_dict[alt_part] is dict:
            for count_part in count_input_dict[alt_part]:
                count_f = count_input_dict[alt_part][count_part]
                with count_f.open("r") as fd:
                    count = pd.read_csv(fd)
                count = update_method(count, **update_method_kwds)
                count_f = count_output_dict[alt_part][count_part]
                with count_f.open("w") as fd:
                    count.to_csv(fd, index=False)
        else:
            count_f = count_input_dict[alt_part]
            with count_f.open("r") as fd:
                count = pd.read_csv(fd)
            count = update_method(count, **update_method_kwds)
            count_f = count_output_dict[alt_part]
            with count_f.open("w") as fd:
                count.to_csv(fd, index=False)
