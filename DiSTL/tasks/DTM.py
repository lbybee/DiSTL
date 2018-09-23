import luigi
import os


class DTMTask(luigi.Task):
    """base class for handling DTM tasks"""

    doc_partitions = luigi.ListParameter()
    term_partitions = luigi.ListParameter()
    count_partitions = luigi.ListParameter()
    processes = luigi.IntParameter()
    in_data_dir = luigi.Parameter()
    out_data_dir = luigi.Parameter()


    def requires(self):

        return DTMTask(doc_partitions=self.doc_partitions,
                       term_partitions=self.term_partitions,
                       count_partitions=self.count_partitions,
                       out_data_dir=self.in_data_dir,
                       in_data_dir=self.in_data_dir,
                       processes=self.processes)


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
            f = os.path.join(self.out_data_dir, "doc_id_%s.csv" % doc_part)
            f = luigi.LocalTarget(f)
            res["doc_id"][doc_part] = f

        for term_part in self.term_partitions:
            f = os.path.join(self.out_data_dir, "term_id_%s.csv" % term_part)
            f = luigi.LocalTarget(f)
            res["term_id"][term_part] = f

        count_part_bool = len(self.count_partitions) > 0

        for doc_part in self.doc_partitions:
            res["count"][doc_part] = {}
            for term_part in self.term_partitions:
                if count_part_bool:
                    res["count"][doc_part][term_part] = {}
                    for count_part in self.count_partitions:
                        f = os.path.join(self.out_data_dir,
                                         "count_%s_%s_%s.csv" % (count_part,
                                                                 doc_part,
                                                                 term_part))
                        f = luigi.LocalTarget(f)
                        res["count"][doc_part][term_part][count_part] = f
                else:
                    f = os.path.join(self.out_data_dir,
                                     "count_%s_%s.csv" % (doc_part,
                                                          term_part))
                    f = luigi.LocalTarget(f)
                    res["count"][doc_part][term_part] = f

        return res
