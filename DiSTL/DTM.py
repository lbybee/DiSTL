"""
This is the core class for supporting a DTM based around dask-dataframes.

Essentially the key here is to recognize that a sparse matrix can be
represented well by a trio of data-frames with mapping indices
"""
from .DTM_part_methods import *
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


def _prep_flist(doc_globstring, term_globstring, count_globstring):
    """prepares lists of file names to help with writing updates

    Parameters
    ----------
    doc_globstring : str
        globstring corresponding to doc_df
    term_globstring : str
        globstring corresponding to term_df
    count_globstring : str
        globstring corresponding to count_df

    Returns
    -------
    tuple of base file names and diffs
    """

    # TODO it should be possible to optimize this considerably further

    # get file lists
    doc_flist = glob.glob(doc_globstring)
    term_flist = glob.glob(term_globstring)
    count_flist = glob.glob(count_globstring)

    # get base names
    doc_flist = [os.path.basename(f) for f in doc_flist]
    term_flist = [os.path.basename(f) for f in term_flist]
    count_flist = [os.path.basename(f) for f in count_flist]

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

    return doc_flist, term_flist, count_flist, doc_diff, term_diff, count_diff


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

    def __init__(self, doc_df, term_df, count_df, doc_index, term_index):

        self.doc_df = doc_df
        self.term_df = term_df
        self.count_df = count_df

        self.doc_index = doc_index
        self.term_index = term_index

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
            raise ValueError("repartition currently only supports shrinking \
                              the existing number of partitions.  Therefore \
                              new npartitions must be less than existing.")

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


    def map_doc(self, func, alt=None, term=False, count=False, **kwargs):
        """maps the provided method over the doc partitions

        Parameters
        ----------
        func : python function
            method to apply to each doc partition
        alt : str or None
            if provided, this corresponds to a dask-df with comparable
            partitions to doc_df (currently contained in kwargs), the
            underlying df for each partition will be passed into func
            along with doc_df
        term : bool
            whether or not the term_df partitions will be passed
            into func along with doc_df (as a list)
        count : bool
            whether or not the comparable count_df partitions will be passed
            into func along with doc_df (as a list)

        Returns
        -------
        None

        Notes
        -----
        We assume that the doc partitions are bigger than the term
        partitions, therefore the outer loop will be handled by dask and
        any additional partitions (over counts or terms) will be passed in
        as lists of values
        """

        if alt is not None:
            alt_df = kwargs.pop(alt)
            alt_del = alt_df.to_delayed()

        doc_del = self.doc_df.to_delayed()
        term_del = self.term_df.to_delayed()
        count_del = self.count_df.to_delayed()

        Dp, Vp = self.npartitions

        del_l = []
        del_fn = delayed(func)

        for i, doc_i in enumerate(doc_del):
            if alt is not None:
                kwargs[alt] = alt_del[i]
            if term:
                kwargs["term_df"] = term_del
            if count:
                kwargs["count_df"] = count_del[(i*Vp):((i+1)*Vp)]
            del_l.append(del_fn(doc_i, **kwargs))

        self.doc_df = dd.from_delayed(del_l)


    def map_term(self, func, alt=None, doc=False, count=False, **kwargs):
        """maps the provided method over the term partitions

        Parameters
        ----------
        func : python function
            method to apply to each term partition
        alt : str or None
            if provided, this corresponds to a dask-df with comparable
            partitions to term_df (currently contained in kwargs), the
            underlying df for each partition will be passed into func
            along with term_df
        doc : bool
            whether or not the doc_df partitions will be passed
            into func along with term_df
        count : bool
            whether or not the comparable count_df partitions will be passed
            into func along with term_df

        Returns
        -------
        None

        Notes
        -----
        We assume that the term partitions are smaller than the doc
        partitions so if we need doc or count info for func, we will do
        the outer loop over term partitions without using delayed and then
        reserve delayed/parallel estimation for within the function
        """

        if alt is not None:
            alt_df = kwargs.pop(alt)
            alt_del = alt_df.to_delayed()

        term_del = self.term_df.to_delayed()

        Dp, Vp = self.npartitions

        del_l = []

        # if we don't need the counts and docs we can do this completely
        # in parallel, otherwise we need to save the parallel component
        # for the inner loop
        if not (doc and count):
            for i, term_i in enumerate(term_del):
                if alt is not None:
                    kwargs[alt] = alt_del[i]
                del_l.append(delayed(func)(term_i, **kwargs))

        else:

            # TODO there likely is a better way to do this part...
            del_fn = delayed(lambda x: x)

            for i, term_i in enumerate(term_del):
                if alt is not None:
                    kwargs[alt] = alt_del[i]
                if doc:
                    kwargs["doc_df"] = self.doc_df
                if count:
                    kwargs["count_df"] = self.count_df.partitions[i:(Dp*Vp):Vp]
                term_i = func(term_i, **kwargs)
                del_l.append(del_fn(term_id))

        self.term_df = dd.from_delayed(del_l)


    def map_count(self, func, alt=None, alt_doc=None, alt_term=None,
                  doc=False, term=False, **kwargs):
        """maps the provided method over the count partitions

        Parameters
        ----------
        func : python function
            method to apply to each count partition
        alt : str or None
            if provided, this corresponds to a dask-df with comparable
            partitions to count_df (currently contained in kwargs), the
            underlying df for each partition will be passed into func
            along with count_df
        alt_doc : str or None
            if provided, this corresponds to a dask-df with comparable
            partitions to doc_df (currently contained in kwargs), the
            underlying df for each partition will be passed into func
            along with count_df
        alt_term : str or None
            if provided, this corresponds to a dask-df with comparable
            partitions to term_df (currently contained in kwargs), the
            underlying df for each partition will be passed into func
            along with count_df
        doc : bool
            whether or not the comparable doc_df partition will be passed
            into func along with count_df
        term : bool
            whether or not the comparable term_df partition will be passed
            into func along with count_df

        Returns
        -------
        None
        """

        # prep delayed values for additional metadata
        if alt is not None:
            alt_df = kwargs.pop(alt)
            alt_del = alt_df.to_delayed()
        if alt_doc is not None:
            alt_doc_df = kwargs.pop(alt_doc)
            alt_doc_del = alt_doc_df.to_delayed()
        if alt_term is not None:
            alt_term_df = kwargs.pop(alt_term)
            alt_term_del = alt_term_df.to_delayed()

        doc_del = self.doc_df.to_delayed()
        term_del = self.term_df.to_delayed()
        count_del = self.count_df.to_delayed()

        Dp, Vp = self.npartitions

        del_l = []
        del_fn = delayed(func)

        for i, doc_i in enumerate(doc_del):
            for j, term_j in enumerate(term_del):
                q = (i * Vp) + j
                if alt is not None:
                    kwargs[alt] = alt_del[q]
                if alt_doc is not None:
                    kwargs[alt_doc] = alt_doc_del[i]
                if alt_term is not None:
                    kwargs[alt_term] = alt_term_del[j]
                if doc:
                    kwargs["doc_df"] = doc_i
                if term:
                    kwargs["term_df"] = term_j
                del_l.append(del_fn(count_del[q], **kwargs))

        self.count_df = dd.from_delayed(del_l)


    def map_axis(self, func, axis, **kwargs):
        """wrapper function for generally mapping over axes"""

        if axis == "doc":
            self.map_doc(func, **kwargs)
        elif axis == "term":
            self.map_term(func, **kwargs)
        elif axis == "count":
            self.map_count(func, **kwargs)


    def reset_index(self, doc=False, term=False, count=False):
        """resets each index, this should be run after any series of
        operations which may change the data in DTM

        Parameters
        ----------
        doc : bool
            whether or not updates have been made to doc_df which change
            the number of elements in each partition
        term : bool
            whether or not updates have been made to term_df which change
            the number of elements in each partition
        count : bool
            whether or not updates have been made to count_df which change
            the number of elements in each partition

        Returns
        -------
        None
        """

        # TODO currently we have to touch the metadata twice, ideally we
        # should only have to touch it once here, but that is going to require
        # that the we can map functions which return multiple values...

        # if nothing was updated raise warning
        if not (doc or term or count):
            raise ValueError("reset_index should not be called when no data \
                              has been changed")

        # first subset dfs based on updates
        if doc or term:
            self.map_axis(int_count, axis="count", doc=True, term=True,
                          doc_index=self.doc_index,
                          term_index=self.term_index)
        if count:
            self.map_axis(int_doc, axis="doc", count=True,
                          doc_index=self.doc_index)
            self.map_axis(int_term, axis="term", count=True,
                          term_index=self.term_index)

        # now generate cumulative counts to share new df size between
        # partitions
        doc_fn = lambda x: x[[self.doc_index]].count()
        doc_count = self.doc_df.map_partitions(doc_fn).cumsum()
        term_fn = lambda x: x[[self.term_index]].count()
        term_count = self.term_df.map_partitions(term_fn).cumsum()

        #  now reset
        self.map_axis(reset_ind_count, axis="count", doc=True, term=True,
                      alt_doc="doc_count", alt_term="term_count",
                      doc_count=doc_count, term_count=term_count,
                      doc_index=self.doc_index, term_index=self.term_index)
        self.map_axis(reset_ind_mdata, axis="doc", alt="doc_count",
                      doc_count=doc_count, doc_index=self.doc_index)
        self.map_axis(reset_ind_term, axis="term", alt="term_count",
                      term_count=term_count, term_index=self.term_index)


    def merge(self, new_df, axis="doc", **kwargs):
        """merge another dask-dataframe along the specified axis"""

        if axis == "doc":
            comp_df = self.doc_df
            lab = "doc_df"
        elif axis == "term":
            comp_df = self.term_df
            lab = "term_df"
        elif axis == "count":
            comp_df = self.count_df
            lab = "count_df"
        if comp_df.npartitions != new_df.npartitions:
            raise ValueError("new_df and %s must share same npartitions" %
                             lab)

        kwargs["new_df"] = new_df
        self.map_axis(merge_part, alt="new_df", axis=axis, **kwargs)


    def add(self, new_dtm, **kwargs):
        """sum two DTMs with same doc and terms"""

        # all metadata must be the same between DTMs to add
        if self.doc_df != new_dtm.doc_df:
            raise ValueError("main and new DTM must share same doc_df")
        elif self.term_df != new_dtm.term_df:
            raise ValueError("main and new DTM must share same term_df")

        kwargs["new_count"] = new_dtm.count_df
        self.map_axis(add_part, alt="new_count", axis="count", **kwargs)


def read_csv(in_data_dir=None, doc_globstring=None, term_globstring=None,
             count_globstring=None, doc_index="doc_id", term_index="term_id",
             blocksize=None, **kwargs):
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
    blocksize : scalar or None
        blocksize for dask dfs.  Given that we want our partitions to align
        we default this to None (so each partition corresponds to a file)

    Returns
    -------
    populated DTM object
    """

    if in_data_dir:
        if doc_globstring or term_globstring or count_globstring:
            raise ValueError("If in_data_dir provided don't provide \
                              globstrings")
        else:
            doc_globstring = os.path.join(in_data_dir, "doc_id_*.csv")
            term_globstring = os.path.join(in_data_dir, "term_id_*.csv")
            count_globstring = os.path.join(in_data_dir, "count_*.csv")

    flists = _prep_flist(doc_globstring, term_globstring, count_globstring)
    doc_flist, term_flist, count_flist, doc_d, term_d, count_d = flists

    # load doc id info
    doc_flist.sort()
    if len(doc_flist) == 1:
        doc_df = dd.from_pandas(pd.read_csv(doc_flist[0], **kwargs),
                                npartitions=1)
    else:
        doc_df = dd.read_csv(doc_flist, blocksize=blocksize, **kwargs)

    # load term id info
    term_flist.sort()
    if len(term_flist) == 1:
        term_df = dd.from_pandas(pd.read_csv(term_flist[0], **kwargs),
                                     npartitions=1)
    else:
        term_df = dd.read_csv(term_flist, blocksize=blocksize, **kwargs)

    # load counts
    count_flist.sort()
    if len(count_flist) == 1:
        count_df = dd.from_pandas(pd.read_csv(count_flist[0], **kwargs),
                                  npartitions=1)
    else:
        count_df = dd.read_csv(count_flist, blocksize=blocksize, **kwargs)

    dtm = DTM(doc_df=doc_df, term_df=term_df, count_df=count_df,
              doc_index=doc_index, term_index=term_index)

    return dtm
