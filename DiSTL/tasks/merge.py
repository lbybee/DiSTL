from .utilities import _copy_id
from .DTM import DTMTask
import multiprocessing
import pandas as pd
import luigi


def _process_partition(part_input_file, count_input_dict,
                       metadata_input_file, part_output_file,
                       count_output_dict, merge_method,
                       merge_method_kwds, ax_label):
    """merges the metadata with the part input and writes the output

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
    merge_method : function
        method for merging part_id and metadata
    merge_method_kwds : dict-like
        key-words passed to merge_method
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
    part_id = merge_method(part_id, metadata_df, **merge_method_kwds)

    # write new part_id
    with part_output_file.open("w") as fd:
        part_id.to_csv(fd, index=False)

    # update counts
    for alt_part in count_input_dict:
        if count_input_dict[alt_part] is dict:
            for count_part in count_input_dict[alt_part]:
                count_f = count_input_dict[alt_part][count_part]
                with count_f.open("r") as fd:
                    count = pd.read_csv(fd)
                count = pd.read_csv(count_f.open("r"))
                count = count[count[ax_label].isin(part_id[ax_label])]
                count_f = count_output_dict[alt_part][count_part]
                with count_f.open("w") as fd:
                    count.to_csv(fd, index=False)
        else:
            count_f = count_input_dict[alt_part]
            with count_f.open("r") as fd:
                count = pd.read_csv(fd)
            count_f = count_input_dict[alt_part]
            count = pd.read_csv(count_f.open("r"))
            count = count[count[ax_label].isin(part_id[ax_label])]
            count_f = count_output_dict[alt_part]
            with count_f.open("w") as fd:
                count.to_csv(fd, index=False)



class mergeMetadataTask(DTMTask):
    """takes a DTM and metadata files (partitioned over <X>_parts)
    and returns a merged DTM

    Parameters
    ----------
    merge_method_kwds : dict-like or None
        key-words passed to merge method
    ax_label : str
        label for column over merge axis (doc_id or term_id)
    """

    merge_method_kwds = luigi.DictParameter(None)
    ax_label = luigi.Parameter()

    def merge_method(self, part_id, metadata, **kwds):

        raise NotImplementedError("merge_method required to merge")


    def run(self):

        # handle merge axis
        if self.ax_label != "doc_id" and self.ax_label != "term_id":
            raise ValueError("ax_label must be either doc_id or term_id")

        if self.ax_label == "term_id":
            raise NotImplementedError("currently term axis doesn't work")

        if self.ax_label == "doc_id":
            k_partitions = self.doc_partitions
            alt_partitions = self.term_partitions
            alt_label = "term_id"
        elif self.ax_label == "term_id":
            k_partitions = self.term_partitions
            alt_partitions = self.doc_partitions
            alt_label = "doc_id"

        # initialize merge_method_kwds if None
        if self.merge_method_kwds is None:
            self.merge_method_kwds = {}

        # load input
        input_dict = self.input()
        metadata_input_dict = input_dict[0]
        DTM_input_dict = input_dict[0]

        # load output
        output_dict = self.output()

        # initialize pool
        pool = multiprocessing.Pool(self.processes)

        pool.starmap(_process_partition,
                     [(DTM_input_dict[self.ax_label][k_part],
                       DTM_input_dict["count"][k_part],
                       metadata_input_dict[k_part],
                       output_dict[self.ax_label][k_part],
                       output_dict["count"][k_part],
                       self.merge_method,
                       self.merge_method_kwds,
                       self.ax_label)
                      for k_part in k_partitions])

        # copy term_ids
        pool.starmap(_copy_id, [(DTM_input_dict[alt_label][alt_part],
                                 output_dict[alt_label][alt_part])
                                for alt_part in alt_partitions])
