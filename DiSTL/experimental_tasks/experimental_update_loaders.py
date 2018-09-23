
# ------------ #
# cout Loaders #
# ------------ #

def load_base_counts(data_dir, doc_part, term_partitions):
    """method for loading the counts for the corresponding doc_part

    Parameters
    ----------
    data_dir : str
        location where data is stored
    doc_part : str
        label for doc partition
    term_partitions : list
        list of term partitions

    Returns
    -------
    df containing counts for all term partitions
    """

    # load all counts
    counts = pd.DataFrame([], columns=["doc_id", "term_id", "count"])
    for term_part in term_partitions:
        t_counts = pd.read_csv(os.path.join(data_dir,
                                            "count_%s_%s.csv" % (term_part,
                                                                 doc_part)))
        counts = pd.concat([counts, t_counts])

    # TODO why does pd.concat convert the counts data types to objects?
    # convert them back to ints...
    counts["doc_id"] = counts["doc_id"].astype(int)
    counts["term_id"] = counts["term_id"].astype(int)
    counts["count"] = counts["count"].astype(int)

    return counts



# -------------- #
# doc_id Loaders #
# -------------- #

def load_base_doc_id(data_dir, doc_part):

    raise NotImplementedError("load_base_doc_id not implemented yet")


def load_crsp_doc_id(data_dir, doc_part):
    """loads the crsp doc ids

    Parameters
    ----------
    data_dir : str
        location where data is stored
    doc_part : str
        label for doc partition

    Returns
    -------
    doc_id
    """

    # load doc id
    doc_id = pd.read_csv(os.path.join(data_dir, "doc_id_%s.csv" % doc_part))

    # generate SIC industry group codes (first 3 digits of SIC code)
    doc_id["SIC_industry_group"] = doc_id["SICCD"].astype(str).str[:3]
    doc_id["SIC_industry_group"] = doc_id["SIC_industry_group"]

    return doc_id
