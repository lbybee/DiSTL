from DiSTL.query.global_methods import create_temp_term_table, \
                                       drop_temp_term_table, \
                                       count_query
from datetime import datetime
from .DTM import DTMTask
import multiprocessing
import psycopg2
import luigi
import os


class queryTask(DTMTask):
    """extract DTM from Postgres database

    Parameters
    ----------
    doc_partitions : list
        list of doc partitions (e.g. dates)
    term_partitions  : list
        list of term partitions (e.g. 1gram, 2gram,...)
    count_partitions : list
        list of count partitions (e.g. headline, body)
    out_data_dir : str
        location where output is to be stored
    doc_columns : list
        list of column names for doc_id files
    term_columns : list
        list of column names for term_id files
    count_columns : list
        list of column names for count files
    log_file : None or str
        if None location where log is written when done
    processes : int
        number of processes for multiprocessing pool
    schema : str
        name of schema
    db_kwds : dict-like
        key-words to pass to postgresl DB connection
    schema_type : str
        type of db schema, used for importing type specific SQL methods
        (e.g. DJ, NYT...)
    ngram_term_threshold_dict : dict-like
        dictionary mapping each ngram to a term threshold
    ngram_stop_words_dict : dict-like
        dictionary mapping each ngram to stop words list
    ngram_regex_stop_words_dict : dict-like
        dictionary mapping each ngram to regex stop words list
    gen_full_doc_sql_kwds : dict-like or None
        key words passed to gen_full_doc_sql

    Writes
    ------
    1. doc_id files
    2. count files
    3. term_id files
    """

    # TODO add support for length zero count_partitions

    out_data_dir = luigi.Parameter()
    in_data_dir = out_data_dir
    doc_columns = luigi.ListParameter()
    term_columns = luigi.ListParameter(["term", "term_id"])
    count_columns = luigi.ListParameter(["doc_id", "term_id", "count"])
    log_file = luigi.Parameter()
    processes = luigi.IntParameter()
    schema = luigi.Parameter()
    db_kwds = luigi.DictParameter()
    schema_type = luigi.Parameter()
    ngram_term_threshold_dict = luigi.DictParameter()
    ngram_stop_words_dict = luigi.DictParameter()
    ngram_regex_stop_words_dict = luigi.DictParameter()
    gen_full_doc_sql_kwds = luigi.DictParameter()


    def requires(self):

        return None


    def run(self):

        # import schema type methods
        if self.schema_type == "DJ":
            from DiSTL.query.DJ_methods import gen_count_sql, \
                                               gen_full_doc_sql
        else:
            raise NotImplementedError("schema_type: %s not available" %
                                      self.schema_type)

        # wipe out temporary tables if they exist
        for term_part in self.term_partitions:
            drop_temp_term_table(self.schema, self.db_kwds, term_part)

        # wipe out log
        open(self.log_file, "w")

        # initialize multiprocessing
        pool = multiprocessing.Pool(self.processes)

        # get output dict
        output = self.output()

        # create tmp term tables and save
        pool.starmap(create_temp_term_table,
                     [(self.schema, self.db_kwds,
                       output["term_id"][term_part], term_part,
                       self.term_columns,
                       self.ngram_term_threshold_dict[term_part],
                       self.ngram_stop_words_dict[term_part],
                       self.ngram_regex_stop_words_dict[term_part],
                       self.log_file)
                      for term_part in self.term_partitions])

        # count (and doc) query
        pool.starmap(count_query,
                     [(self.schema, self.db_kwds,
                       output["doc_id"][doc_part], output["count"][doc_part],
                       doc_part, self.term_partitions, self.count_partitions,
                       self.doc_columns, self.count_columns,
                       gen_full_doc_sql, gen_count_sql,
                       self.gen_full_doc_sql_kwds, self.log_file)
                      for doc_part in self.doc_partitions])

        # drop tmp tables
        pool.starmap(drop_temp_term_table, [(self.schema, self.db_kwds,
                                             term_part) for term_part in
                                            self.term_partitions])
