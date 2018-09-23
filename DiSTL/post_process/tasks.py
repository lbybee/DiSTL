from .reindex import load_term_id, counter, reindexer
import multiprocessing
import luigi
import os


# TODO this doesn't necessarily fit with the luigi approach...
class baseDTMTask(luigi.ExternalTask):
    """a minimal class to provide DTM update methods as required

    Parameters
    ----------
    doc_partitions : list
        list of partitions which doc_ids are split over (e.g. dates)
        these should correspond to elements in doc_id files
        (e.g. doc_id_<doc_part_label>.csv)
    term_partitions : list
        list of partitions for terms (e.g. ngrams)
        these should correspond to elements in term_id files
        (e.g. term_id_<agg_part_label>_<term_part_label>.csv)
    count_partitions : None or list
        if not None this contains a list of additional partitions
        who's counts should be aggreagated during the reindexing
        process (e.g. headlines and body) the count files should be
        of the form:
        count_<agg_part_label>_<term_part_label>_<doc_part_label>.csv
    data_dir : str
        location where files are stored
    """

    def output(self):
        """the output should be a set of doc_id, term_id and count files
        in the data_dir, they are of the form

        doc_id_<doc_part>.csv
        term_id_<term_part>.csv
        count_<count_part>_<doc_part>_<term_part>.csv

        and the dictionary returned by output maps each partition to the
        corresponding file
        (e.g. output["doc_id"][doc_part] -> data_dir/doc_id_<doc_part>.csv)
        """

        res = {"doc_id": {}, "term_id": {}, "count": {}}

        for doc_part in self.doc_partitions:
            f = os.path.join(self.data_dir, "doc_id_%s.csv" % doc_part)
            f = luigi.LocalTarget(f)
            res["doc_id"][doc_part] = f

        for term_part in self.term_partitions:
            f = os.path.join(self.data_dir, "term_id_%s.csv" % term_part)
            f = luigi.LocalTarget(f)
            res["term_id"][term_part] = f

        for doc_part in self.doc_partitions:
            res["count"][doc_part] = {}
            for term_part in self.term_partitions:
                res["count"][doc_part][term_part] = {}
                for count_part in self.count_partitions:
                    f = os.path.join(self.data_dir,
                                     "count_%s_%s_%s.csv" % (count_part,
                                                             doc_part,
                                                             term_part))
                    f = luigi.LocalTarget(f)
                    res["count"][doc_part][term_part][count_part] = f

        return res


class reindexTask(luigi.Task):
    """reindex a CSV DTM, currently this consists of

    1. merge with document metadata
    2. reset-indices such that they are continous
    3. aggregate count_partitons into one partition
       (collapse headlines and body into one count)

    Parameters
    ----------
    source_data_dir : str
        location of input files
    agg_count_dir : str
        location where temporary aggregate count files will be stored
    out_data_dir : str
        location where results are stored
    processes : int
        number of processes for multiprocessing
    load_doc_id_method : function
        method for generating doc ids
    doc_partitions : list
        list of partitions which doc_ids are split over (e.g. dates)
        these should correspond to elements in doc_id files
        (e.g. doc_id_<doc_part_label>.csv)
    term_partitions : list
        list of partitions for terms (e.g. ngrams)
        these should correspond to elements in term_id files
        (e.g. term_id_<agg_part_label>_<term_part_label>.csv)
    count_partitions : None or list
        if not None this contains a list of additional partitions
        who's counts should be aggreagated during the reindexing
        process (e.g. headlines and body) the count files should be
        of the form:
        count_<agg_part_label>_<term_part_label>_<doc_part_label>.csv
    metadata_f : None or str
        location of metadata file if exists
    load_metadata_method : function or None
        method which takes the metadata_f and doc_partitions
        list and returns a partitioned dataframe split along
        the doc_partitions corresponding to the metadata
    metadata_method_kwds : dict-like or None
        key-words to pass to load_metadata_method
    metadata_merge_columns : None or list
        list of columns to merge metadata to doc_id
    sort_columns : list or None
        list of columns which we'd like to sort the resulting doc_ids on

    Writes
    ------
    1. new term ids
    2. new doc ids (including metadata)
    3. new counts
    """

    source_data_dir = luigi.Parameter()
    out_data_dir = luigi.Parameter()
    length_file = luigi.Parameter()
    processes = luigi.IntParameter()
    doc_partitions = luigi.ListParameter()
    term_partitions = luigi.ListParameter()
    count_partitions = luigi.ListParameter()
    metadata_f = luigi.OptionalParameter()
    load_metadata_method = luigi.OptionalParameter()
    metadata_method_kwds = luigi.OptionalParameter()
    metadata_merge_columns = luigi.OptionalParameter()
    sort_columns = luigi.OptionalParameter()


    def requires(self):
        """this is just to make sure that the source files exist, it doesn't
        totally fit the luigi approach"""

        baseDTMTask(self.doc_partitions,
                    self.term_partitions,
                    self.count_partitions,
                    self.in_data_dir)


    def run(self):

        # initialize multiprocessing pool
        pool = multiprocessing.Pool(self.processes)

        # initialize kwds
        if self.doc_id_method_kwds is None:
            self.doc_id_method_kwds = {}
        if self.metadata_method_kwds is None:
            self.metadata_method_kwds = {}

        # load permno ticker map
        if self.metadata_f:
            md_parts = self.load_metadata_method(self.metadata_f,
                                                 self.doc_partitions,
                                                 **self.metadata_method_kwds)
        else:
            md_parts = [None for part in self.doc_partitions]

        # load input and output dicts
        input_dict = self.input()
        output_dict = self.output()

        # handle term ids/indices
        term_id_offset_dict = self.load_term_id(input_dict["term_id"],
                                                output_dict["term_id"],
                                                self.term_partitions)

        # generate agg counts
        open(self.length_file, "w")
        pool.starmap(agg_length, [(input_dict["doc_id"][doc_part],
                                   load_doc_id_method,
                                   doc_id_method_kwds
                        source_data_dir, agg_count_dir, part,
                                load_doc_id_method, doc_id_method_kwds,
                                metadata_df_partitions[i],
                                metadata_merge_columns)
                                for i, part in enumerate(doc_partitions)])

        # generate reindexed docs and counts
        pool.starmap(reindexer, [(source_data_dir, agg_count_dir, out_data_dir,
                                  load_doc_id_method, doc_id_method_kwds,
                                  part, term_partitions, term_id_offset_dict,
                                  term_agg_partitions, metadata_df_partitions[i],
                                  metadata_merge_columns, sort_columns)
                                 for i, part in enumerate(doc_partitions)])


    def output(self):
        """the output should be a set of doc_id, term_id and count files
        in the data_dir, they are of the form

        doc_id_<doc_part>.csv
        term_id_<term_part>.csv
        count_<doc_part>_<term_part>.csv

        and the dictionary returned by output maps each partition to the
        corresponding file
        (e.g. output["doc_id"][doc_part] -> data_dir/doc_id_<doc_part>.csv)
        """

        res = {"doc_id": {}, "term_id": {}, "count": {}}

        for doc_part in self.doc_partitions:
            f = os.path.join(self.data_dir, "doc_id_%s.csv" % doc_part)
            f = luigi.LocalTarget(f)
            res["doc_id"][doc_part] = f

        for term_part in self.term_partitions:
            f = os.path.join(self.data_dir, "term_id_%s.csv" % term_part)
            f = luigi.LocalTarget(f)
            res["term_id"][term_part] = f

        for doc_part in self.doc_partitions:
            res["count"][doc_part] = {}
            for term_part in self.term_partitions:
                f = os.path.join(self.data_dir,
                                 "count_%s_%s.csv" % (doc_part, term_part))
                f = luigi.LocalTarget(f)
                res["count"][doc_part][term_part] = f

        return res
