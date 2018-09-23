from DiSTL.reindex import reindexTask
from DiSTL.DTM import DTMCSVTask
import dask.dataframe as dd
import multiprocessing


def _sum_counts(input_dict, output_file):
    """take multiple count partitions and sum into one count file

    Parameters
    ----------
    input_dict : dictionary
        mapping from count_partitions to count partition files for corresponding
        doc_part and term_part
    output_file : LocalTarget
        count file for corresponding doc_part and term_part

    Returns
    -------
    None

    Writes
    ------
    updated count file
    """

    count = dd.read_csv([input_dict[k].open("r") for k in input_dict])
    count = count.groupby(["doc_id", "term_id"])["count"].sum().compute()
    count = count.reset_index()

    count.to_csv(output_file.open("w"), index=False)


class collapseTask(DTMTask):
    """collapses count partitions into one partition/layer"""


    def run(self):

        if len(self.count_partitions) == 0:
            raise ValueError("Can't collapse count partitions if none exist")

        # initialize pool
        pool = multiprocess.Pool(self.processes)

        # load input_dict and output_dict
        input_dict = self.input()
        output_dict = self.output()

        # sum the count file for each doc_part and term_part over
        # the count_partitions
        pool.starmap(_sum_counts, [(input_dict["count"][doc_part][term_part],
                                    output_dict["count"][doc_part][term_part])
                                   for doc_part in self.doc_partitions
                                   for term_part in self.term_partitions])


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
                                 "count_%s_%s.csv" % (doc_part,
                                                      term_part))
                f = luigi.LocalTarget(f)
                res["count"][doc_part][term_part] = f

        return res
