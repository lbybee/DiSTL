"""
This is the core class for supporting a DTM based around dask-dataframes.

Essentially the key here is to recognize that a sparse matrix can be
represented well by a trio of data-frames with mapping indices
"""
from dask import delayed
import dask.dataframe as dd
import pandas as pd
import numpy as np
import difflib
import glob
import os

# TODO currently, we aren't returning new copies of the DTM for the various
# methods.  This approach does not align with dask/pandas and should probably
# be changed.


# NOTE this function is currently not used
def _prep_count_map(doc_globstring, term_globstring, count_globstring):
    """prepares a numpy array mapping each count partition to a corresponding
    doc and term parititon
    """

    # TODO it should be possible to optimize this considerably further

    # get file lists
    doc_flist = glob.glob(doc_globstring)
    term_flist = glob.glob(term_globstring)
    count_flist = glob.glob(count_globstring)

    # extract patterns to populate count_map
    doc_diff = ["".join([r.replace("+ ", "") for r in
                difflib.ndiff(doc_globstring, f) if "+" in r])
                for f in doc_flist]
    term_diff = ["".join([r.replace("+ ", "") for r in
                 difflib.ndiff(term_globstring, f) if "+" in r])
                 for f in term_flist]
    count_diff = ["".join([r.replace("+ ", "") for r in
                  difflib.ndiff(count_globstring, f) if "+" in r])
                  for f in count_flist]

    res = []

    for diff in count_diff:
        doc_ind = [i for i, d in enumerate(doc_diff) if d in diff][0]
        term_ind = [i for i, d, in enumerate(term_diff) if d in diff][0]

        res.append([doc_ind, term_ind])

    return np.array(res)


def _add_part(main_count_i, new_count_i, **kwargs):
    """takes a count_df partition from the main DTM and a count_df partition
    from the new DTM and sums them, returning the sum

    Parameters
    ----------
    main_count_i : pandas df
        main count_df partition
    new_count_i : pandas df
        new count_df partition

    Returns
    -------
    updated main_count_i
    """

    if "fill_value" in kwargs:
        raise ValueError("fill_value NAN default overwritten by 0")

    # TODO can we generalize this set and reset procedure for multiindices?

    main_count_i = main_count_i.set_index([self.doc_index, self.term_index])
    new_count_i = new_count_i.set_index([self.doc_index, self.term_index])

    main_count_i = main_count_i.add(new_count_i, fill_value=0, **kwargs)
    main_count_i = main_count_i.reset_index()

    return main_count_i


def _reset_count_part(count_ji, doc_j, term_i, doc_index, term_index):
    """resets the doc_index and term_index for the corresponding counts

    Parameters
    ----------
    count_ji : pandas dataframe
        count_df for current count (doc & term) partition
    doc_j : pandas df
        doc_df for current doc partition
    term_i : pandas df
        term_df for current term partition
    doc_index : str
        label for doc index
    term_index : str
        label for term index

    Returns
    -------
    updated count
    """

    doc_count_val = doc_j.shape[0]
    term_count_val = term_i.shape[0]

    doc_index_map = doc_j[[doc_index]]
    doc_index_map["n_%s" % doc_index] = doc_index_map.index + doc_count_val
    doc_index_map.index = doc_index_map[doc_index]
    doc_index_map = doc_index_map["n_%s" % doc_index]

    term_index_map = term_j[[term_index]]
    term_index_map["n_%s" % term_index] = (term_index_map.index +
                                           term_count_val)
    term_index_map.index = term_index_map[term_index]
    term_index_map = term_index_map["n_%s" % term_index]

    count_ji[doc_index] = count_ji[doc_index].map(doc_index_map)
    count_ji[term_index] = count_ji[term_index].map(term_index_map)

    return count_ji


def _reset_mdata_part(mdata_q, mdata_index):
    """resets the mdata index for corresponding mdata

    Parameters
    ----------
    mdata_q : pandas dataframe
        dataframe for metadata for current parition
    mdata_index : str
        label for mdata index

    Returns
    -------
    updated mdata_q
    """

    mdata_count_val = mdata_q.shape[0]

    mdata_index_map = mdata_q[[mdata_index]]
    mdata_index_map["n_%s" % mdata_index] = (mdata_index_map.index +
                                             mdata_count_val)
    mdata_index_map.index = mdata_index_map[mdata_index]
    mdata_index_map = mdata_index_map["n_%s" % mdata_index]

    mdata_q[mdata_index] = mdata_q[mdata_index].map(mdata_index_map)

    return mdata_q


def _merge_count_part(count_ji, mdata_q, mdata_index):
    """constrain the counts to whatever is left after the mdata merge

    Parameters
    ----------
    count_ji : pandas dataframe
        dataframe for the corresponding jth doc partition and ith term
        partition
    mdata_q : pandas dataframe
        metadata for axis over which merge is happening
    mdata_index : str
        label for axis index

    Returns
    -------
    updated count_ji (constrained to results from merge)
    """

    count_ji = count_ji[count_ji[mdata_index].isin(mdata_q[mdata_index])]

    return count_ji


class DTM(object):
    """Core class for handling DTM data

    Parameters
    ----------
    doc_df : dask-dataframe
        dataframe containing doc metadata and id/index
    term_df : dask-dataframe
        dataframe containing term metadata and id/index
    count_df : dask-dataframe
        dataframe containing counts and doc+term id/index
    doc_index : str
        label for doc id/index
    term_index : str
        label for term id/index
    set_index : bool
        whether to set the indices for doc_df and term_df to their
        corresponding index labels

    Attributes
    ----------
    doc_df : dask-dataframe
        dataframe containing doc metadata and id/index
    term_df : dask-dataframe
        dataframe containing term metadata and id/index
    count_df : dask-dataframe
        dataframe containing counts and doc+term id/index
    doc_index : str
        label for doc id/index
    term_index : str
        label for term id/index
    """

    def __init__(self, doc_df, term_df, count_df, doc_index, term_index,
                 set_index=False):

        self.doc_df = doc_df
        self.term_df = term_df
        self.count_df = count_df

        self.doc_index = doc_index
        self.term_index = term_index

        if set_index:
            self.doc_df = self.doc_df.set_index(doc_index)
            self.term_df = self.term_df.set_index(term_index)

        self.npartitions = (doc_df.npartitions,
                            term_df.npartitions)


    def to_csv(self, out_data_dir=None, doc_globstring=None,
               term_globstring=None, count_globstring=None, **kwargs):
        """writes the current DTM to the specified files

        Parameters
        ----------
        out_data_dir : str or None
            location directory where results should be stored
        doc_globstring : str or None
            globstring for doc_df
        term_globstring : str or None
            globstring for term_df
        count_globstring : str or None
            globstring for count_df

        Returns
        -------
        None
        """

        if out_data_dir:
            if doc_globstring or term_globstring or count_globstring:
                raise ValueError("If out_data_dir provided don't provide \
                                  globstrings")
            else:
                doc_globstring = os.path.join(out_data_dir, "doc_id_*.csv")
                term_globstring = os.path.join(out_data_dir, "term_id_*.csv")
                count_globstring = os.path.join(out_data_dir, "count_*.csv")

        self.doc_df.to_csv(doc_globstring, index=False, **kwargs)
        self.term_df.to_csv(term_globstring, index=False, **kwargs)
        self.count_df.to_csv(count_globstring, index=False, **kwargs)


    def add(self, dtm, **kwargs):
        """adds another DTM to the current DTM

        Parameters
        ----------
        dtm : DTM instance
            an additional DTM which we wish to add

        Returns
        -------
        None
        """

        # all metadata must be the same between DTMs to add
        if self.count_map != dtm.count_map:
            raise ValueError("main and new DTM must share same count_map")
        elif self.doc_df != dtm.doc_df:
            raise ValueError("main and new DTM must share same doc_df")
        elif self.term_df != dtm.term_df:
            raise ValueError("main and new DTM must share same term_df")

        # update counts
        main_count_del = self.count_df.to_delayed()
        new_count_del = dtm.count_df.to_delayed()
        del_l = [delayed(_add_part)(main_count_i, new_count_i)
                 for main_count_i, new_count_i in
                 zip(main_count_del, new_count_del)]
        self.count_df = dd.from_delayed(del_l)


    def reset_index(self):
        """resets each index, this should be run after any series of
        operations which may change the data in DTM"""

        # get delayed values
        doc_del = self.doc_df.to_delayed()
        term_del = self.term_df.to_delayed()
        count_del = self.count_df.to_delayed()

        # prep fns
        cdel_fn = delayed(_reset_count_part)
        mdel_fn = delayed(_reset_mdata_part)

        # update count indices
        del_l = []

        q = 0
        for doc_j in doc_del:
            for term_i in term_del:
                count_ji = count_del[q]
                del_l.append(cdel_fn(count_ji, doc_j, term_i, self.doc_index,
                                     self.term_index))
                q += 1
        self.count_df = dd.from_delayed(del_l)

        # update doc indices
        del_l = [mdel_fn(doc_j, self.doc_index) for doc_j in doc_del]
        self.doc_df = dd.from_delayed(del_l)

        # update term indices
        del_l = [mdel_fn(term_i, self.term_index) for term_i in term_del]
        self.term_df = dd.from_delayed(del_l)


    def repartition(self, npartitions):
        """repartition the DTM

        Parameters
        ----------
        npartitions : scalar
            number of partitions for new DTM

        Returns
        -------
        None

        Notes
        -----
        1. This is only over the doc axis
        2. This only supports shrinking the number of partitions
        """

        # TODO currently this only supports shrinking the number of
        # partitions
        if npartitions > self.doc_df.npartitions:
            raise ValueError("npartitions must be less than existing")

        doc_fn = lambda x: x[[self.doc_index]].count()
        doc_count = self.doc_df.map_partitions(doc_fn).compute()
        D = doc_count.sum()
        stp = D / npartitions
        doc_cum_count = doc_count.cumsum()

        # prep partition info
        partitions = []
        for n in range(npartitions):
            chk = len(doc_cum_count[doc_cum_count < n * stp])
            partitions.append(chk)
        partitions.append(len(doc_cum_count))

        term_part = self.term_df.npartitions

        # init fn
        del_fn = delayed(pd.concat)

        # prep delayed data
        doc_del = self.doc_df.to_delayed()
        count_del = self.count_df.to_delayed()

        doc_del_l = []
        count_del_l = []

        # collapse partitions
        for n in range(1, npartitions + 1):

            n_start = partitions[n-1]
            n_stop = partitions[n]
            doc_del_n = doc_del[n_start:n_stop]

            doc_del_l.append(del_fn(doc_del_n))

            for t in range(term_part):

                t_start = (n_start * term_part + t)
                t_stop = (n_stop * term_part + t)
                t_stp = term_part

                count_del_nt = count_del[t_start:t_stop:t_stp]
                count_del_l.append(del_fn(count_del_nt))

        self.doc_df = dd.from_delayed(doc_del_l)
        self.count_df = dd.from_delayed(count_del_l)

        self.npartitions = (self.doc_df.npartitions,
                            self.term_df.npartitions)


    def update_doc(self, method, **kwargs):
        """applies an update to the doc axis"""

        return None


    def update_term(self, method, **kwargs):
        """applies an update to the term axis"""

        return None


    def merge(self, new_df, axis="doc", **kwargs):
        """merge another dask-dataframe along the specified axis

        Parameters
        ----------
        new_df : dataframe
            the data-frame we are merging on the axis
        axis : str
            label for axis (currently should be doc or term)

        Returns
        -------
        None
        """

        # TODO test new_df with pandas

        # init fn
        del_fn = delayed(_merge_count_part)

        # merge data
        if axis == "doc":
            self.doc_df = self.doc_df.merge(new_df, **kwargs)
        elif axis == "term":
            self.term_df = self.term_df.merge(new_df, **kwargs)
        else:
            raise ValueError("Unsupported axis %s" % axis)

        # get delayed dfs
        doc_del = self.doc_df.to_delayed()
        term_del = self.term_df.to_delayed()
        count_del = self.count_df.to_delayed()

        # update counts
        del_l = []
        q = 0
        for doc_j in doc_del:
            for term_i in term_del:
                count_ji = count_del[q]
                if axis == "doc":
                    del_l.append(del_fn(count_ji, doc_j, doc_index))
                elif axis == "term":
                    del_l.append(del_fn(count_ji, term_i, term_index))
                q += 1
        self.count_df = dd.from_delayed(del_l)


def read_csv(in_data_dir=None, doc_globstring=None, term_globstring=None,
             count_globstring=None, doc_index="doc_id", term_index="term_id",
             set_index=False, blocksize=None, **kwargs):
    """reads the csvs for each partition and populates DTM

    Parameters
    ----------
    in_data_dir : str or None
        if provided, we assume that all the files in this directory correspond
        to a DTM and populate globstrings accordingly
    doc_globstring : str or None
        globstring for doc_df files
    term_globstring : str or None
        globstring for term_df files
    count_globstring : str or None
        globstring for count files
    doc_index : str
        label for doc axis index
    term_index : str
        label for term axis index
    set_index : bool
        whether or not we set the index for doc_df and term_df to doc_index
        and term_index.  Note that this currently doesn't do much...
    blocksize : scalar or None
        blocksize for dask dfs.  Given that we want our partitions to align
        we default this to None (so each partition corresponds to a file)

    Returns
    -------
    populated DTM object
    """

    # TODO handle set_index, should we keep this?  Can we do multiindex?

    if in_data_dir:
        if doc_globstring or term_globstring or count_globstring:
            raise ValueError("If in_data_dir provided don't provide \
                              globstrings")
        else:
            doc_globstring = os.path.join(in_data_dir, "doc_id_*.csv")
            term_globstring = os.path.join(in_data_dir, "term_id_*.csv")
            count_globstring = os.path.join(in_data_dir, "count_*.csv")

    # load doc id info
    doc_flist = glob.glob(doc_globstring)
    doc_flist.sort()
    if len(doc_flist) == 1:
        doc_df = dd.from_pandas(pd.read_csv(doc_flist[0], **kwargs),
                                npartitions=1)
    else:
        doc_df = dd.read_csv(doc_flist, blocksize=blocksize, **kwargs)

    # load term id info
    term_flist = glob.glob(term_globstring)
    term_flist.sort()
    if len(term_flist) == 1:
        term_df = dd.from_pandas(pd.read_csv(term_flist[0], **kwargs),
                                     npartitions=1)
    else:
        term_df = dd.read_csv(term_flist, blocksize=blocksize, **kwargs)

    # load counts
    count_flist = glob.glob(count_globstring)
    count_flist.sort()
    if len(count_flist) == 1:
        count_df = dd.from_pandas(pd.read_csv(count_flist[0], **kwargs),
                                  npartitions=1)
    else:
        count_df = dd.read_csv(count_flist, blocksize=blocksize, **kwargs)

    dtm = DTM(doc_df=doc_df, term_df=term_df, count_df=count_df,
              doc_index=doc_index, term_index=term_index,
              set_index=set_index)

    return dtm
