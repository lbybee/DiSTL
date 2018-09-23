from .DTM import DTMTask
import luigi


class updateTask(luigi.Task):

    def run(self):

        input_dict = self.input()

        print(input_dict)
