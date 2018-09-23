import luigi


class queryTask(luigi.ExternalTask):


    doc_partitions = luigi.ListParameter()
    term_partitions = luigi.ListParameter()
    count_partitions = luigi.ListParameter()
    data_dir = luigi.Parameter()

    def output(self):

        if len(self.count_partitions) > 0:
            part_labels = ["_".join([count_part, doc_part, term_part])
                           for count_part in self.count_partitions
                           for doc_part in self.doc_partitions
                           for term_part in self.term_partitions]
        else:
            part_labels = ["_".join([doc_part, term_part])
                           for doc_part in self.doc_partitions
                           for term_part in self.term_partitions]
        f_l = ["count_%s.csv" % p_l for p_l in part_labels]
        f_l += [os.path.join(self.data_dir, "doc_id_%s.csv" % doc_part
                for doc_part in self.doc_partitions]
        f_l += [os.path.join(self.data_dir, "term_id_%s.csv" % term_part
                for term_part in self.term_partitions]
        return f_l
