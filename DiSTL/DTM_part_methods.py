"""
methods called by DTM mappers
"""
import pandas as pd


def int_doc(doc_df, count_df, doc_index):
    """updates doc_df to only contain indices currently in count_df

    Parameters
    ----------
    doc_df : pandas df
        doc_df partition
    count_df : list of dataframes
        corresponding count_dfs over term partitions
    doc_index : str
        label for doc index

    Returns
    -------
    updated doc_df
    """

    count_df = pd.concat(count_df)
    ind = count_df[doc_index].unique()
    doc_df = doc_df[doc_df[doc_index].isin(ind)]
    doc_df = doc_df.reset_index(drop=True)

    return doc_df


def int_term(term_df, count_df, term_index):
    """updates term_df to only contain indices currently in count_df

    Parameters
    ----------
    term_df : pandas df
        term_df partition
    count_df : dask df
        dask df for corresponding partitions
    term_index : str
        label for term index

    Returns
    -------
    updated term_df
    """

    ind = count_df[term_index].unique().compute()
    term_df = term_df[term_df[term_index].isin(ind)]
    term_df = term_df.reset_index(drop=True)

    return term_df


def int_count(count_df, doc_df, term_df, doc_index, term_index):
    """updates count_df to only contain indices currently in doc and term dfs

    Parameters
    ----------
    count_df : pandas df
        count_df partition
    doc_df : pandas df
        corresponding doc_df partition
    term_df : pandas df
        corresponding term_df partition
    doc_index : str
        label for document index
    term_index : str
        label for term index

    Returns
    -------
    updated count_df
    """

    count_df = count_df[count_df[doc_index].isin(doc_df[doc_index])]
    count_df = count_df[count_df[term_index].isin(term_df[term_index])]
    count_df = count_df.reset_index(drop=True)

    return count_df


def reset_ind_mdata(mdata_df, mdata_count, mdata_index):
    """reset the index for the mdata partition

    mdata_df : pandas df
        mdata_df partition
    mdata_count : single value pandas df
        cumsum of mdata counts including current partition
    mdata_index : str
        label for mdata index

    Returns
    -------
    updated mdata_df
    """

    mdata_count_val = mdata_count[0] - mdata_df.shape[0]

    mdata_ind_map = mdata_df[[mdata_index]]
    mdata_ind_map = mdata_ind_map.reset_index(drop=True)
    mdata_ind_map["n_%s" % mdata_index] = (mdata_ind_map.index +
                                           mdata_count_val)
    mdata_ind_map.index = mdata_ind_map[mdata_index]
    mdata_ind_map = mdata_ind_map["n_%s" % mdata_index]

    mdata_df[mdata_index] = mdata_df[mdata_index].map(mdata_ind_map)

    return mdata_df


def reset_ind_count(count_df, doc_df, term_df, doc_count, term_count,
                    doc_index, term_index):
    """reset the index along the count parititions

    Parameters
    ----------
    count_df : pandas df
        count_df partition
    doc_df : pandas df
        corresponding doc_df partition
    term_df : pandas df
        corresponding term_df partition
    doc_count : single value pandas df
        cumsum of doc counts including current partition
    term_count : single value pandas df
        cumsum of term counts including current partition
    doc_index : str
        label for doc index
    term_index : str
        label for term index

    Returns
    -------
    updated count_df
    """

    # doc_count is the cumsum for the
    doc_count_val = doc_count[0] - doc_df.shape[0]
    term_count_val = term_count[0] - term_df.shape[0]

    doc_ind_map = doc_df[[doc_index]]
    doc_ind_map = doc_ind_map.reset_index(drop=True)
    doc_ind_map["n_%s" % doc_index] = doc_ind_map.index + doc_count_val
    doc_ind_map.index = doc_ind_map[doc_index]
    doc_ind_map = doc_ind_map["n_%s" % doc_index]

    term_ind_map = term_df[[term_index]]
    term_ind_map = term_ind_map.reset_index(drop=True)
    term_ind_map["n_%s" % term_index] = term_ind_map.index + term_count_val
    term_ind_map.index = term_ind_map[term_index]
    term_ind_map = term_ind_map["n_%s" % term_index]

    count_df[doc_index] = count_df[doc_index].map(doc_ind_map)
    count_df[term_index] = count_df[term_index].map(term_ind_map)

    return count_df


def merge_part(main_df, new_df, **kwargs):
    """take a partition for the main df and a comparable partition from the
    new df and merge

    Parameters
    ----------
    main_df : pandas df
        main_df partition
    new_df : pandas df
        new_df partition

    Returns
    -------
    updated main_df
    """

    main_df = main_df.merge(new_df, **kwargs)

    return main_df


def add_part(main_count, new_count, **kwargs):
    """takes a count_df partition from the main DTM and a count_df partition
    from the new DTM and sums them, returning the sum

    Parameters
    ----------
    main_count : pandas df
        main count_df partition
    new_count : pandas df
        new count_df partition

    Returns
    -------
    updated main_count_i
    """

    # TODO can we generalize this set and reset procedure for multiindices?

    main_count = main_count.set_index([self.doc_index, self.term_index])
    new_count = new_count.set_index([self.doc_index, self.term_index])

    main_count = main_count.add(new_count, fill_value=0, **kwargs)
    main_count = main_count.reset_index()

    return main_count_i


def sample_part(df, **kwargs):
    """wrapper function to sample from df"""

    df = df.sample(**kwargs)
    df = df.sort_index()

    return df
