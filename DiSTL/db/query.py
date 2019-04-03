"""
methods to run query against flat-file db
"""
from coordinator import Coordinator
import pandas as pd
import os


def query(db_dir, out_dir, part_l, doc_cols, term_cols,
          docfn=None, termfn=None, countfn=None,
          doc_ind="doc_id", term_ind="term_id", doc_fp="doc_%s.csv",
          term_fp="term.csv", count_fp="count_%s.csv", **kwds):
    """method to run query over flat-file db

    Parameters
    ----------
    db_dir : str
        directory where csv files corresponding to text db are located
    out_dir : str
        location where the resulting DTM will be written
    part_l : list
        list of partition labels overwhich documents are shared/partitioned
    doc_cols : list
        list of columns to keep for the final doc id table
    term_cols : list
        list of columns to keep for the final term id table
    docfn : function or None
        function which applies any query operations to the provided doc
        metadata data-frame
        This function should return an updated version of the document
        metadata df which is constrained to account for any queries
    termfn : function or None
        function which applies any query operations to the provided term
        metadata data-frame
        This function should return an updated version of the term
        metadata df which is constrained to account for any queries
    countfn : function or None
        function which applies any query operations to the provided count
        data-frame
    doc_ind : str
        label for index column in doc metadata shared with counts
    term_ind : str
        label for index column in term metadata shared with counts
    doc_fp : str
        file pattern which is formatted with part to produce doc metadata path
    term_fp : str
        file pattern corresponding to term metadata
    count_fp : str
        file pattern which is formatted with part to produce count path
    kwds : dict
        additional key words to pass to Coordinator
    """

    coord = Coordinator(**kwds)

    # prep term id
    term = pd.read_csv(os.path.join(db_dir, term_fp))
    if termfn:
        term = termfn(term, db_dir)
    term_map = term.reset_index(drop=True)
    term_map.index.name = "term_ind"
    term_map = term_map.reset_index()
    term_map = term_map.set_index(term_ind)["term_ind"]
    term["term_id"] = term[term_ind].map(term_map)
    if term_cols:
        term = term[term_cols]
    term.to_csv(os.path.join(out_dir, "term.csv"), index=False)

    # run part query
    coord.map(part_query, part_l, db_dir=db_dir, out_dir=out_dir,
              docfn=docfn, countfn=countfn, doc_cols=doc_cols,
              doc_ind=doc_ind, term_ind=term_ind, term_map=term_map,
              doc_fp=doc_fp, count_fp=count_fp,
              gather=True, pure=False)


def part_query(part, db_dir, out_dir, docfn, countfn, doc_cols,
               doc_ind, term_ind, term_map, doc_fp, count_fp):
    """runs query on single partition

    Parameters
    ----------
    part : str
        label for the current partition being queried
    db_dir : str
        directory where csv files corresponding to text db are located
    out_dir : str
        location where the resulting DTM will be written
    doc_cols : list
        list of columns to keep for the final doc id table
    docfn : function or None
        function which applies any query operations to the provided doc
        metadata data-frame
        This function should return an updated version of the document
        metadata df which is constrained to account for any queries
    countfn : function or None
        function which applies any query operations to the provided count
        data-frame
    doc_ind : str
        label for index column in doc metadata shared with counts
    term_ind : str
        label for index column in term metadata shared with counts
    term_map : pandas Series
        map from old term index to new term index which is applied to counts
        (to align them with term metadata)
    doc_fp : str
        file pattern which is formatted with part to produce doc metadata path
    count_fp : str
        file pattern which is formatted with part to produce count path
    """

    # handle docs
    doc = pd.read_csv(os.path.join(db_dir, doc_fp % part))
    if docfn:
        doc = docfn(doc, part, db_dir)
    doc_map = doc.reset_index(drop=True)
    doc_map.index.name = "doc_ind"
    doc_map = doc_map.reset_index()
    doc_map = doc_map.set_index(doc_ind)["doc_ind"]
    doc["doc_id"] = doc[doc_ind].map(doc_map)
    if doc_cols:
        doc = doc[doc_cols]
    doc.to_csv(os.path.join(out_dir, doc_fp % part), index=False)

    # handle counts
    count = pd.read_csv(os.path.join(db_dir, count_fp % part))
    count = count[count[doc_ind].isin(doc_map.index)]
    count = count[count[term_ind].isin(term_map.index)]
    count["term_id"] = count[term_ind].map(term_map)
    count["doc_id"] = count[doc_ind].map(doc_map)
    if countfn:
        count = countfn(count)
    count = count[["doc_id", "term_id", "count"]]
    count.to_csv(os.path.join(out_dir, count_fp % part), index=False)
