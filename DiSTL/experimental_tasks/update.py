from DiSTL.DTM import DTMTask
import multiprocessing
import luigi



def _process_partition(doc_input_file, doc_output_file, count_input_dict,
                       count_output_dict, update_methods,
                       update_method_kwds, update_method_part_kwds
                       term_input_dict=None):
    """applies all provided functions to the current doc_part

    Parameters
    ----------
    doc_input_file : LocalTarget
        location of current doc_id input
    doc_output_file : localTarget
        location of doc_id output
    count_input_dict : dict
        dictionary of dicts or LocalTargets corresponding to input count
        files
    count_output_dict : dict
        dictionary of dicts or LocalTargets corresponding to output count
        files
    update_methods : list
        list of functions to apply to docs
    update_method_kwds : list
        list of dicts where each dict mapps to a update_method
    update_method_part_kwds : list or None
        list of None or dict, if dict these are partition specific
        params passed to update_method
    term_input_dict : dict or None
        dictionary of LocalTargets containing input files for term_ids

    Returns
    -------
    None

    Writes
    ------
    1. updated doc_id
    2. updated counts
    """

    doc_id = pd.read_csv(doc_input_file.open("r"))

    # read counts

    # if term input included read terms

    # update kwds with part specific and term input

    # apply methods
    for method, kwds in zip(update_methods, update_method_kwds):
        doc_id, counts = method(doc_id, counts,...)

    # write doc id
    doc_id.to_csv(doc_output_file.open("w"), index=False)

    # write counts


class updateTask(DTMTask):

    doc_update_methods = luigi.ListParameter()
    doc_update_method_kwds = luigi.ListParameter()
    doc_update_method_part_kwds = luigi.ListParameter()


    def run(self):

        # load input_dict and output_dict
        input_dict = self.input()
        output_dict = self.output()

        # initialize pool
        pool = multiprocessing.Pool(self.processes)

        # apply doc partition updates
        pool.starmap(_process_part, [(input_dict["doc_id"][doc_part],
                                      output_dict["doc_id"][doc_part],
                                      input_dict["count"][doc_part],
                                      output_dict["count"][doc_part],
                                      self.doc_update_methods,
                                      self.doc_update_method_kwds,
                                      self.doc_update_method_part_kwds,
