"""
This is the core class for supporting a DTM based around dask-dataframes.

Essentially the key here is to recognize that a sparse matrix can be
represented well by a trio of data-frames with linked indices and the DTM
class contains sets of common operations for this class of data.
"""
from .DTM_part_methods import *
from dask import delayed
import dask.dataframe as dd
import pandas as pd
import numpy as np
import difflib
import glob
import os


def _prep_fnames(doc_globstring, term_globstring):
    """prepares lists of file names to help with writing updates

    Parameters
    ----------
    doc_globstring : str
        globstring corresponding to doc_df
    term_globstring : str
        globstring corresponding to term_df

    Returns
    -------
    None
    """

    # TODO note that this currently doesn't "truly" find the unique
    # component of each file name.  It compares with the globstring and
    # it may be the case that the resulting files still have some shared
    # component (e.g. if globstring is doc_*.csv and all doc files are
    # called doc_id_*.csv), this should be fixed

    # get file lists
    doc_flist = glob.glob(doc_globstring)
    term_flist = glob.glob(term_globstring)

    doc_flist.sort()
    term_flist.sort()

    # get base names
    doc_flist = [os.path.basename(f) for f in doc_flist]
    term_flist = [os.path.basename(f) for f in term_flist]

    # extract patterns to populate count_map
    doc_fpat = ["".join([r.replace("+ ", "") for r in
                difflib.ndiff(doc_globstring, f) if "+" in r])
                for f in doc_flist]
    term_fpat = ["".join([r.replace("+ ", "") for r in
                 difflib.ndiff(term_globstring, f) if "+" in r])
                 for f in term_flist]

    if len(doc_fpat) == 1:
        doc_fpat = None
    if len(term_fpat) == 1:
        term_fpat = None

    return doc_fpat, term_fpat


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
    doc_fpat : list or None
        file pattern stored to produce doc files, will be:
        <out_dir>/doc_<doc_fpat>[i]
    term_fpat : list or None
        file pattern stored to produce term files, will be:
        <out_dir>/term_<term_fpat>[j]

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
    doc_fpat : list or None
        file pattern stored to produce doc files, will be:
        <out_dir>/doc_<doc_fpat>[i]
    term_fpat : list or None
        file pattern stored to produce term files, will be:
        <out_dir>/term_<term_fpat>[j]

    Notes
    -----
    If file patterns are stored the count files will be
    <out_dir>/count_<doc_fpat>[i]_<term_fpat>[j]
    """

    def __init__(self, doc_df, term_df, count_df, doc_index, term_index,
                 doc_fpat=None, term_fpat=None):

        self.doc_df = doc_df
        self.term_df = term_df
        self.count_df = count_df

        self.doc_index = doc_index
        self.term_index = term_index

        self.doc_fpat = doc_fpat
        self.term_fpat = term_fpat

        self.npartitions = (doc_df.npartitions,
                            term_df.npartitions)


    def copy(self):
        """generates a copy of the DTM and returns that

        Returns
        -------
        copy of current DTM
        """

        dtm = DTM(doc_df=self.doc_df.copy(),
                  term_df=self.term_df.copy(),
                  count_df=self.count_df.copy(),
                  doc_index=self.doc_index,
                  term_index=self.term_index,
                  doc_fpat=self.doc_fpat,
                  term_fpat=self.term_fpat)

        return dtm


    def persist(self):
        """persists the underlying data-frames

        Returns
        -------
        copy of current DTM
        """

        dtm = self.copy()

        dtm.doc_df = dtm.doc_df.persist()
        dtm.term_df = dtm.term_df.persist()
        dtm.count_df = dtm.count_df.persist()

        return dtm



    def to_csv(self, out_dir=None, doc_urlpath=None,
               term_urlpath=None, count_urlpath=None, **kwargs):
        """writes the current DTM to the specified files

        Parameters
        ----------
        out_dir : str or None
            location directory where results should be stored
        doc_urlpath : str or None
            urlpath for doc_df
        term_urlpath : str or None
            urlpath for term_df
        count_urlpath : str or None
            urlpath for count_df
        """

#        self = self.persist()

        if not out_dir:
            out_dir = "."
        if self.doc_fpat:
            doc_fpat = self.doc_fpat
        else:
            doc_fpat = [("%d" % d).zfill(2) for d in
                        range(self.npartitions[0])]
        if self.term_fpat:
            term_fpat = self.term_fpat
        else:
            term_fpat = [("%d" % d).zfill(2) for d in
                         range(self.npartitions[1])]

        if not doc_urlpath:
            doc_urlpath = [os.path.join(out_dir, "doc_%s.csv" % f)
                           for f in doc_fpat]
        if not term_urlpath:
            term_urlpath = [os.path.join(out_dir, "term_%s.csv" % f)
                            for f in term_fpat]
        if not count_urlpath:
            count_urlpath = [os.path.join(out_dir, "count_%s_%s.csv" %
                                          (d_f, t_f))
                             for d_f in doc_fpat
                             for t_f in term_fpat]

        self.doc_df.to_csv(doc_urlpath, index=False, **kwargs)
        self.term_df.to_csv(term_urlpath, index=False, **kwargs)
        self.count_df.to_csv(count_urlpath, index=False, **kwargs)


    def repartition_doc(self, npartitions):
        """repartition the DTM along the doc axis

        Parameters
        ----------
        npartitions : scalar
            number of partitions for new DTM

        Returns
        -------
        updated DTM

        Notes
        -----
        - Only supports shrinking the number of partitions
        - We attempt to optimize the partitions so that the actual docs
          are spread as evenly as possible.  Therefore we base the new
          partitions on the doc counts within the old partitions.
        """

        dtm = self.copy()

        if npartitions > dtm.doc_df.npartitions:
            raise ValueError("repartition currently only supports shrinking \
                              the existing number of partitions.  Therefore \
                              new npartitions must be less than existing.")

        doc_fn = lambda x: x[[dtm.doc_index]].count()
        doc_count = dtm.doc_df.map_partitions(doc_fn).compute()
        D = doc_count.sum()
        stp = D / npartitions
        doc_cum_count = doc_count.cumsum()

        # prep partition info
        partitions = []
        for n in range(npartitions):
            chk = len(doc_cum_count[doc_cum_count < n * stp])
            partitions.append(chk)
        partitions.append(len(doc_cum_count))

        term_part = dtm.term_df.npartitions

        # init fn
        del_concat = delayed(pd.concat)

        # prep delayed data
        doc_del = dtm.doc_df.to_delayed()
        count_del = dtm.count_df.to_delayed()

        doc_del_l = []
        count_del_l = []

        # collapse partitions
        for n in range(1, npartitions + 1):

            n_start = partitions[n-1]
            n_stop = partitions[n]
            doc_del_n = doc_del[n_start:n_stop]

            doc_del_l.append(del_concat(doc_del_n))

            for t in range(term_part):

                t_start = (n_start * term_part + t)
                t_stop = (n_stop * term_part + t)
                t_stp = term_part

                count_del_nt = count_del[t_start:t_stop:t_stp]
                count_del_l.append(del_concat(count_del_nt))

        dtm.doc_df = dd.from_delayed(doc_del_l)
        dtm.count_df = dd.from_delayed(count_del_l)

        # if we are storing fname patterns combine these as well
        if dtm.doc_fpat is not None:

            n_doc_fpat = []
            for n in range(1, npartitions + 1):
                n_start = partitions[n-1]
                n_stop = partitions[n]

                doc_fpat_part = dtm.doc_fpat[n_start:n_stop]
                if len(doc_fpat_part) > 1:
                    n_doc_fpat.append(doc_fpat_part[0] + "_T_" +
                                      doc_fpat_part[-1])
                elif len(doc_fpat_part) == 1:
                    n_doc_fpat.append(doc_fpat_part[0])
                else:
                    raise ValueError("doc_fpat doesn't align with parititons")

            dtm.doc_fpat = n_doc_fpat

        dtm.npartitions = (dtm.doc_df.npartitions,
                            dtm.term_df.npartitions)

        return dtm


    def repartition_term(self):
        """repartitions the DTM along the term axis

        Returns
        -------
        updated DTM

        Notes
        -----
        This only support collapsing the term partitions to 1
        """

        dtm = self.copy()

        # collapsing terms is simple
        dtm.term_df = dtm.term_df.repartition(npartitions=1)

        # now we need to collapse each term partition
        del_concat = delayed(pd.concat)

        count_del = dtm.count_df.to_delayed()

        Dp, Vp = dtm.npartitions

        count_del_l = []

        for d in range(Dp):

            count_del_nt = count_del[(d * Vp):((d + 1) * Vp)]
            count_del_l.append(del_concat(count_del_nt))

        dtm.count_df = dd.from_delayed(count_del_l)

        # now reset fpatterns if provided
#        if dtm.term_fpat is not None:
#            dtm.term_fpat = [dtm.term_fpat[0] + "_T_" + dtm.term_fpat[1]]
        # since we've collapsed to term counts we shouldn't have a pattern
        # any longer
        dtm.term_fpat = None

        # finally reset npartitions
        dtm.npartitions = (dtm.doc_df.npartitions,
                            dtm.term_df.npartitions)

        return dtm


    def repartition(self, npartitions=None, axis="doc"):
        """repartitions along the provided axis

        Parameters
        ----------
        npartitions : scalar or None
            number of new partitions, must not be None if axis == doc
        axis : str
            label for axis over which to repartition

        Returns
        -------
        updated DTM
        """

        if axis == "doc":
            if npartitions is None:
                raise ValueError("If repartitioning over docs, npartitions \
                                  must be provided")
            else:
                return self.repartition_doc(npartitions)

        elif axis == "term":
            if npartitions is not None:
                raise ValueError("If repartitioning over terms, npartitions \
                                  should not be provided")
            else:
                return self.repartition_term()


    def map_doc(dtm, func, alt=None, term=False, count=False,
                kwds_l=None, **kwargs):
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
        kwds_l : list or None
            if provided, this corresponds to a list of key-words which
            are partition specific, therefore it should be the same length
            as the term partitions

        Returns
        -------
        updated DTM

        Notes
        -----
        We assume that the doc partitions are bigger than the term
        partitions, therefore the outer loop will be handled by dask and
        any additional partitions (over counts or terms) will be passed in
        as lists of values
        """

        dtm = dtm.copy()

        Dp, Vp = dtm.npartitions

        if kwds_l is None:
            kwds_l = [{} for i in range(Dp)]
        if len(kwds_l) != Dp:
            raise ValueError("kwds_l needs to be same length as doc \
                              partitions")

        if alt is not None:
            alt_df = kwargs.pop(alt)
            alt_del = alt_df.to_delayed()

        doc_del = dtm.doc_df.to_delayed()
        term_del = dtm.term_df.to_delayed()
        count_del = dtm.count_df.to_delayed()

        del_l = []
        del_fn = delayed(func)

        for i, doc_i in enumerate(doc_del):
            if alt is not None:
                kwargs[alt] = alt_del[i]
            if term:
                kwargs["term_df"] = term_del
            if count:
                kwargs["count_df"] = count_del[(i*Vp):((i+1)*Vp)]
            kwds = {**kwds_l[i], **kwargs}
            del_l.append(del_fn(doc_i, **kwds))

        dtm.doc_df = dd.from_delayed(del_l)

        return dtm


    def map_term(self, func, alt=None, doc=False, count=False,
                 kwds_l=None, **kwargs):
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
        kwds_l : list or None
            if provided, this corresponds to a list of key-words which
            are partition specific, therefore it should be the same length
            as the term partitions

        Returns
        -------
        updated DTM

        Notes
        -----
        We assume that the term partitions are smaller than the doc
        partitions so if we need doc or count info for func, we will do
        the outer loop over term partitions without using delayed and then
        reserve delayed/parallel estimation for within the function
        """

        dtm = self.copy()

        Dp, Vp = dtm.npartitions

        if kwds_l is None:
            kwds_l = [{} for i in range(Vp)]
        if len(kwds_l) != Vp:
            raise ValueError("kwds_l needs to be same length as term \
                              partitions")

        if alt is not None:
            alt_df = kwargs.pop(alt)
            alt_del = alt_df.to_delayed()

        term_del = dtm.term_df.to_delayed()

        del_l = []

        # if we don't need the counts and docs we can do this completely
        # in parallel, otherwise we need to save the parallel component
        # for the inner loop
        if not (doc or count):
            for i, term_i in enumerate(term_del):
                if alt is not None:
                    kwargs[alt] = alt_del[i]
                kwds = {**kwds_l[i], **kwargs}
                del_l.append(delayed(func)(term_i, **kwds))

        else:

            # TODO there likely is a better way to do this part...
            del_fn = delayed(lambda x: x)

            for i, term_i in enumerate(term_del):
                if alt is not None:
                    kwargs[alt] = alt_del[i]
                if doc:
                    kwargs["doc_df"] = dtm.doc_df
                if count:
                    tmp = dtm.count_df.partitions[i:(Dp*Vp):Vp]
                    kwargs["count_df"] = tmp
                kwds = {**kwds_l[i], **kwargs}
                term_i = func(term_i, **kwds)
                del_l.append(del_fn(term_i))

        dtm.term_df = dd.from_delayed(del_l)

        return dtm


    def map_count(self, func, alt=None, alt_doc=None, alt_term=None,
                  doc=False, term=False, kwds_l=None, **kwargs):
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
        kwds_l : list or None
            if provided, this corresponds to a list of key-words which
            are partition specific, therefore it should be the same length
            as the count partitions

        Returns
        -------
        updated DTM
        """

        dtm = self.copy()

        Dp, Vp = dtm.npartitions

        if kwds_l is None:
            kwds_l = [{} for i in range(Dp * Vp)]
        if len(kwds_l) != (Dp * Vp):
            raise ValueError("kwds_l needs to be same length as count \
                              partitions")

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

        doc_del = dtm.doc_df.to_delayed()
        term_del = dtm.term_df.to_delayed()
        count_del = dtm.count_df.to_delayed()

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
                kwds = {**kwds_l[q], **kwargs}
                del_l.append(del_fn(count_del[q], **kwds))

        dtm.count_df = dd.from_delayed(del_l)

        return dtm


    def map_partitions(self, func, axis="doc", **kwargs):
        """wrapper function for generally mapping over partitions along
        a provided axis

        Parameters
        ----------
        func : python funct
            method to apply to corresponding axis over the partitions
        axis : str
            label for axis to map

        Returns
        -------
        updated DTM
        """

        if axis == "doc":
            return self.map_doc(func, **kwargs)
        elif axis == "term":
            return self.map_term(func, **kwargs)
        elif axis == "count":
            return self.map_count(func, **kwargs)


    def intersect_index(self, doc=False, term=False, count=False):
        """intersects the specified axes to populate any updates which might
        drop observations

        Parameters
        ----------
        doc : bool
            whether to constrain doc_df based on counts
        term : bool
            whether to constrain term_df based on counts
        count : bool
            whether to constrain count_df based on doc_df and term_df

        Returns
        -------
        updated DTM
        """

        dtm = self.copy()

        # first subset dfs based on updates
        if count:
            dtm = dtm.map_partitions(intersect_count, axis="count", doc=True,
                                     term=True, doc_index=dtm.doc_index,
                                     term_index=dtm.term_index)
        if doc:
            dtm = dtm.map_partitions(intersect_doc, axis="doc", count=True,
                                     doc_index=dtm.doc_index)
        if term:
            dtm = dtm.map_partitions(intersect_term, axis="term", count=True,
                                     term_index=self.term_index)

        return dtm


    def reset_index(self):
        """resets each index, this should be run after any series of
        operations which may change the data in DTM where the index matters

        Returns
        -------
        updated DTM
        """

        # TODO currently we have to touch the metadata twice, ideally we
        # should only have to touch it once here, but that is going to require
        # that the we can map functions which return multiple values...

        dtm = self.copy()

        # now generate cumulative counts to share new df size between
        # partitions
        doc_fn = lambda x: x[[dtm.doc_index]].count()
        doc_count = dtm.doc_df.map_partitions(doc_fn).cumsum()
        term_fn = lambda x: x[[dtm.term_index]].count()
        term_count = dtm.term_df.map_partitions(term_fn).cumsum()

        #  now reset
        dtm = dtm.map_partitions(reset_ind_count, axis="count", doc=True,
                                 term=True, alt_doc="doc_count",
                                 alt_term="term_count", doc_count=doc_count,
                                 term_count=term_count,
                                 doc_index=dtm.doc_index,
                                 term_index=dtm.term_index)
        dtm = dtm.map_partitions(reset_ind_mdata, axis="doc",
                                 alt="mdata_count", mdata_count=doc_count,
                                 mdata_index=dtm.doc_index)
        dtm = dtm.map_partitions(reset_ind_mdata, axis="term",
                                 alt="mdata_count", mdata_count=term_count,
                                 mdata_index=dtm.term_index)

        return dtm


    def add(self, new_dtm, **kwargs):
        """sum two DTMs with same doc and terms

        Parameters
        ----------
        new_dtm : DTM instance
            another DTM which we assume has comparable dimensions/partitions
            to the current one

        Returns
        -------
        updated DTM
        """

        # TODO this currently doesn't really compare dataframes

        # all metadata must be the same between DTMs to add
        if len(self.doc_df) != len(new_dtm.doc_df):
            raise ValueError("main and new DTM must share same doc_df")
        elif len(self.term_df) != len(new_dtm.term_df):
            raise ValueError("main and new DTM must share same term_df")

        kwargs["new_count"] = new_dtm.count_df
        return self.map_partitions(add_part, alt="new_count",
                                   axis="count", doc_index=self.doc_index,
                                   term_index=self.term_index, **kwargs)


    def sort(self):
        """sorts the DTM according to doc and term ids

        Returns
        -------
        updated sorted DTM
        """

        dtm = self.copy()

        doc_fn = lambda x: x.sort_values(self.doc_index)
        dtm.doc_df = dtm.doc_df.map_partitions(doc_fn)
        term_fn = lambda x: x.sort_values(self.term_index)
        dtm.term_df = dtm.term_df.map_partitions(term_fn)
        count_fn = lambda x: x.sort_values([self.doc_index, self.term_index])
        dtm.count_df = dtm.count_df.map_partitions(count_fn)

        return dtm


    def merge(self, new_df, axis="doc", **kwargs):
        """merge another dask-dataframe along the specified axis

        Parameters
        ----------
        new_df : dask data-frame
            new dask-dataframe which we wish to merge to one axis
        axis : str
            label for axis which we wish to merge new_df

        Returns
        -------
        updated DTM
        """

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
            raise ValueError("new_df and %s must have same npartitions" % lab)

        kwargs["new_df"] = new_df

        dtm = self.map_partitions(merge_part, alt="new_df", axis=axis,
                                  **kwargs)

        return dtm


    def sample(self, n=None, frac=None, axis="doc", full_sample=True,
               **kwargs):
        """sample from provided axis

        Parameters
        ----------
        n : int, optional
            Number of items from axis to return.  Cannot be used with frace.
            Default = 1 if frac = None
        frac : float, optional
            Fraction of axis items to return.  Cannot be used with n
        axis : str
            label for axis which we wish to merge new_df
        full_index : bool
            whether to sample from each partition indvidually or as a whole

        Returns
        -------
        updated DTM
        """

        if axis == "doc":
            comp_df = self.doc_df
            lab = "doc_df"
        elif axis == "term":
            comp_df = self.term_df
            lab = "term_df"
        elif axis == "count":
            comp_df = self.count_df
            lab = "count_df"

        # if we do full sample then we need to prep n for each partition
        if full_sample:
            if n and frac:
                raise ValueError("n and frac can't both be provided")
            elif n:
                n = int(n / self.npartitions[0])
            elif frac:
                if frac < 0 or frac > 1:
                    raise ValueError("frac must be between 0 and 1")
                else:
                    n = int(frac * len(comp_df))
            kwds_l = [{"n": n} for i in range(comp_df.npartitions)]
        else:
            kwargs["n"] = n
            kwargs["frac"] = frac

        dtm = self.map_partitions(sample_part, axis=axis, kwds_l=kwds_l,
                                  **kwargs)

        # NOTE we need to persist here because sample is stochastic, if we
        # don't persist then future operations may get different results
        dtm = dtm.persist()

        return dtm


    def ttpartition(self, CV_partitions=None, out_dir=None,
                    out_dir_pattern=None):
        """partitions the DTM into a list of tuples where each tuple is
        a training/testing partition pair

        Parameters
        ----------
        CV_partitions : None or scalar
            if provided, we first repartition the data and then the list
            will be of length CV_partitions, otherwise CV_partitions is
            just the current number of partitions
        out_dir : None or str
            we write each partition (instead of returning the list) if
            provided
        out_dir_pattern : None or str
            if provided, this is used as a pattern to fill in test and CV
            labels, otherwise we just use %s_CV%d % (type, part)

        Returns
        -------
        list of DTMs or None
        """

        # if a CV_partition count isn't provided, just partition over the
        # docs
        if CV_partitions is None:
            CV_partitions = self.npartitions[0]
            dtm = self
        else:
            dtm = self.copy()
            dtm = dtm.repartition(CV_partitions)

        if out_dir_pattern is None:
            out_dir_pattern = "%s_CV%d"

        res = []

        Dp, Vp = dtm.npartitions

        for part in range(CV_partitions):

            # prep train_dtm
            cpart_ind = list(range(Dp * Vp))
            dpart_ind = list(range(Dp))

            ctrain_ind = cpart_ind[:(part * Vp)] + cpart_ind[((part+1) * Vp):]
            ctest_ind = cpart_ind[(part * Vp):((part+1) * Vp)]
            dtrain_ind = dpart_ind[:part] + dpart_ind[(part+1):]
            dtest_ind = dpart_ind[part:(part+1)]

            train_dtm = dtm.copy()
            train_dtm.count_df = train_dtm.count_df.partitions[ctrain_ind]
            train_dtm.doc_df = train_dtm.doc_df.partitions[dtrain_ind]
            if train_dtm.doc_fpat is not None:
                train_dtm.doc_fpat = (train_dtm.doc_fpat[:part] +
                                      train_dtm.doc_fpat[(part+1):])
            train_dtm.npartitions = (Dp - 1, Vp)
            train_dtm = train_dtm.reset_index()

            # prep test_dtm
            test_dtm = dtm.copy()
            test_dtm.count_df = test_dtm.count_df.partitions[ctest_ind]
            test_dtm.doc_df = test_dtm.doc_df.partitions[dtest_ind]
            if test_dtm.doc_fpat is not None:
                test_dtm.doc_fpat = test_dtm.doc_fpat[part:(part+1)]
            test_dtm.npartitions = (1, Vp)
            test_dtm = testm_dtm.reset_index()

            # store output
            if out_dir:

                train_dir = os.path.join(out_dir,
                                         out_dir_pattern % ("train", part))
                test_dir = os.path.join(out_dir,
                                        out_dir_pattern % ("test", part))

                os.makedirs(train_dir, exist_ok=True)
                os.makedirs(test_dir, exist_ok=True)

                train_dtm.to_csv(out_dir=train_dir)
                test_dtm.to_csv(out_dir=test_dir)

            else:
                res.append((train_dtm, test_dtm))

        if len(res) > 0:
            return res


def read_csv(in_dir=None, doc_globstring=None, term_globstring=None,
             count_globstring=None, doc_flist=None, term_flist=None,
             count_flist=None, doc_index="doc_id", term_index="term_id",
             keep_fname=True, blocksize=None, **kwargs):
    """reads the csvs for each partition and populates DTM

    Parameters
    ----------
    in_dir : str or None
        if provided, we assume that all the files in this directory
        correspond to a DTM and populate globstrings accordingly
    doc_globstring : str or None
        globstring for doc_df files
    term_globstring : str or None
        globstring for term_df files
    count_globstring : str or None
        globstring for count files
    doc_flist : list or None
        list for doc_df files
    term_list : list or None
        list for term_df files
    count_list : list or None
        list for count files
    doc_index : str
        label for doc axis index
    term_index : str
        label for term axis index
    keep_fname : bool
        whether to keep a record of the filename patterns (for writing
        updated DTM)
    blocksize : scalar or None
        blocksize for dask dfs.  Given that we want our partitions to align
        we default this to None (so each partition corresponds to a file)

    Returns
    -------
    populated DTM object
    """

    if not in_dir:
        in_dir = "."
    if not doc_flist:
        if not doc_globstring:
            doc_globstring = os.path.join(in_dir, "doc_*.csv")
        doc_flist = glob.glob(doc_globstring)
    if not term_flist:
        if not term_globstring:
            term_globstring = os.path.join(in_dir, "term_*.csv")
        term_flist = glob.glob(term_globstring)
    if not count_flist:
        if not count_globstring:
            count_globstring = os.path.join(in_dir, "count_*.csv")
        count_flist = glob.glob(count_globstring)

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

    # prep doc/term fpatterns
    if keep_fname:
        doc_fpat, term_fpat = _prep_fnames(doc_globstring, term_globstring)
    else:
        doc_fpat, term_fpat = None, None

    dtm = DTM(doc_df=doc_df, term_df=term_df, count_df=count_df,
              doc_index=doc_index, term_index=term_index,
              doc_fpat=doc_fpat, term_fpat=term_fpat)

    return dtm