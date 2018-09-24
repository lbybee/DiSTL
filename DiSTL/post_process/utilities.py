"""
helper functions used by multiple tasks
"""
import os


def _copy_id(f_name, in_data_dir, out_data_dir):
    """copies the ids from their input file to output file (since they aren't
    touched when we collapse

    Parameters
    ----------
    f_name : str
        file pattern
    in_data_dir : str
        location of input file
    out_data_dir : str
        location of output file

    Returns
    -------
    None

    Writes
    ------
    id files in new location
    """

    with open(os.path.join(in_data_dir, f_name), "r") as in_fd:
        with open(os.path.join(out_data_dir, f_name), "w") as out_fd:
            out_fd.write(in_fd.read())
