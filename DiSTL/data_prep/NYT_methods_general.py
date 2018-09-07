import json


def doc_generator(f):
    """generates documents from the NYT json raw files

    Parameters
    ----------
    f : str
        file location of json documents

    Yields
    ------
    document object
    """

    with open(f, "r") as fd:
        data = json.load(fd)
        for d in data:
            yield d
