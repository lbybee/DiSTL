from DiSTL.DTM import DTMCSVTask
import multiprocessing
import luigi


class metadataTask(DTMCSVTask):
    """base class method for metadata methods"""

    metadata_method_l = luigi.ListParameter()
    metadata_method_kwds_l = luigi.ListParameter()


class mergeDocMetadataTask(metadataTask):

    def run(self):

        return None

class mergeTermMetadataTask(metadataTask):

    def run(self):

        return None


class constrainDocMetadataTask(metadataTask):

    def run(self):

        return None

class constrainTermMetadataTask(metadataTask):

    def run(self):

        return None


class updateDocMetadataTask(metadataTask):

    def run(self):

        return None

class updateTermMetadataTask(metadataTask):

    def run(self):

        return None
