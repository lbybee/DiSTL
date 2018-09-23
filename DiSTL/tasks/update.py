from .update_methods import _process_base_partition, \
                            _process_metadata_partition, \
                            _process_count_partition
from .utilities import _copy_id
from .DTM import DTMTask
import multiprocessing
import pandas as pd
import luigi


class updateTask(DTMTask):
    """takes a DTM and metadata files (partitioned over <X>_parts)
    and returns a updated DTM

    Parameters
    ----------
    update_method_kwds : dict-like
        key-words passed to update method
    ax_label : str
        label for column over update axis (doc_id or term_id)
    """

    update_method_kwds = luigi.DictParameter()
    ax_label = luigi.Parameter()
    process_type = luigi.Parameter()

    def update_method(self, part_id, **kwds):

        raise NotImplementedError("update_method required to update")


    def run(self):

        # check that there aren't multiple count partitions
        if len(self.count_partitions) > 0:
            raise ValueError("updateTask does not support count_part")

        # check that process types are met
        if (self.process_type != "base" and
            self.process_type != "metadata" and
            self.process_type != "count"):
            raise ValueError("Unsupported process_type: %s" %
                             self.process_type)

        # load input and prep metadata if provided
        input_val = self.input()
        if type(input_val) == list:
            metadata_input_dict = input_val[0]
            DTM_input_dict = input_val[1]
        else:
            DTM_input_dict = input_val

        # load output
        output_dict = self.output()

        # handle update axis
        if self.ax_label != "doc_id" and self.ax_label != "term_id":
            raise ValueError("ax_label must be either doc_id or term_id")

        if self.ax_label == "doc_id":
            main_partitions = self.doc_partitions
            alt_partitions = self.term_partitions
            alt_label = "term_id"

        elif self.ax_label == "term_id":
            main_partitions = self.term_partitions
            alt_partitions = self.doc_partitions
            alt_label = "doc_id"
            # we have to reshape the input/output dicts if term_id
            # is the main partition because by default doc_id is default
            n_DTM_input_dict = {}
            n_DTM_input_dict["doc_id"] = DTM_input_dict["doc_id"]
            n_DTM_input_dict["term_id"] = DTM_input_dict["term_id"]
            n_output_dict = {}
            n_output_dict["doc_id"] = output_dict["doc_id"]
            n_output_dict["term_id"] = output_dict["term_id"]
            n_DTM_input_dict["count"] = {}
            n_output_dict["count"] = {}
            for term_part in self.term_partitions:
                n_DTM_input_dict[term_part] = {}
                n_output_dict[term_part] = {}
                for doc_part in self.doc_partitions:
                    t = DTM_input_dict["count"][doc_part][term_part]
                    n_DTM_input_dict["count"][term_part][doc_part] = t
                    t = output_dict["count"][doc_part][term_part]
                    n_output_dict["count"][term_part][doc_part] = t
            DTM_input_dict = n_DTM_input_dict
            output_dict = n_output_dict

        # initialize pool
        pool = multiprocessing.Pool(self.processes)

        if self.process_type == "base":

            pool.starmap(_process_base_partition,
                         [(DTM_input_dict[self.ax_label][main_part],
                           DTM_input_dict["count"][main_part],
                           output_dict[self.ax_label][main_part],
                           output_dict["count"][main_part],
                           self.update_method,
                           self.update_method_kwds,
                           self.ax_label)
                          for main_part in main_partitions])

        elif self.process_type == "metadata":

            pool.starmap(_process_metadata_partition,
                         [(DTM_input_dict[self.ax_label][main_part],
                           DTM_input_dict["count"][main_part],
                           metadata_input_dict[main_part],
                           output_dict[self.ax_label][main_part],
                           output_dict["count"][main_part],
                           self.update_method,
                           self.update_method_kwds,
                           self.ax_label)
                          for main_part in main_partitions])

        elif self.process_type == "count":

            pool.starmap(_process_count_partition,
                         [(DTM_input_dict[self.ax_label][main_part],
                           DTM_input_dict["count"][main_part],
                           DTM_input_dict[alt_label],
                           output_dict[self.ax_label][main_part],
                           output_dict["count"][main_part],
                           self.update_method,
                           self.update_method_kwds,
                           self.ax_label)
                          for main_part in main_partitions])

        # copy alt ids
        pool.starmap(_copy_id, [(DTM_input_dict[alt_label][alt_part],
                                 output_dict[alt_label][alt_part])
                                for alt_part in alt_partitions])
