from DiSTL.query import query_wrapper
from DiSTL.reindex import reindex_wrapper, collapse_wrapper
from DiSTL.metadata import merge_wrapper, update_wrapper
import luigi


class DTMTask(luigi.Task):
    """base class for handling DTM tasks"""

    doc_partitions = luigi.ListParameter()
    term_partitions = luigi.ListParameter()
    count_partitions = luigi.Parameter()
    in_data_dir = luigi.Parameter()
    out_data_dir = luigi.Parameter()
    processes = luigi.Parameter()


class queryTask(DTMTask):
    """runs a query against the postgres database and builds a DTM"""

    db_kwds = luigi.DictParameter()

    def run(self):

        return query_wrapper(**kwds)

    def output(self):

        return None


class remoteQueryTask(queryTask):
    """runs a query on a remote machine and syncs the output to the local
    machine"""

    def run(self):

        return query_wrapper(**kwds)

    def output(self):

        return None


class reindexTask(DTMTask):
    """reindex the partitions produced by the query such that the indices
    are continous"""

    def requires(self):

        return queryTask()

    def run(self):

        return None

    def output(self):

        return None


class collapseCountPartitionTask(DTMTask):
    """collapses count partitions into one layer"""

    def require(self):

        return reindexTask()

    def run(self):

        return None

    def output(self):

        return None


class mergeMetadataTask(DTMTask):
    """merge metadata into the id files"""

    def require(self):

        return reindexTask()

    def run(self):

        return None

    def output(self):

        return None


class updateMetadataTask(DTMTask):
    """update the metadata based on counts"""

    def require(self):

        return reindexTask()

    def run(self):

        return None

    def output(self):

        return None


class denormalizeTask(DTMTask):
    """collapse the indices into the counts"""

    def require(self):

        return reindexTask()

    def run(self):

        return None

    def output(self):

        return None
