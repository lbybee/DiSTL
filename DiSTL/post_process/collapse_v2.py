from .utilities import _copy_id
from .DTM import DTMTask
import dask.dataframe as dd
import multiprocessing
import luigi
import os


def _sum_counts(input_dict, output_file):
    """take multiple count partitions and sum into one count file

    Parameters
    ----------
    input_dict : dictionary
        mapping from count_partitions to count partition files for
        corresponding doc_part and term_part
    output_file : LocalTarget
        count file for corresponding doc_part and term_part

    Returns
    -------
    None

    Writes
    ------
    updated count file
    """

    count = dd.read_csv([input_dict[k].path for k in input_dict])
    count = count.groupby(["doc_id", "term_id"])["count"].sum().compute()
    count = count.reset_index()

    with output_file.open("w") as fd:
        count.to_csv(fd, index=False)
