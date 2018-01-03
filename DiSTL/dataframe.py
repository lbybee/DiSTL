"""
Includes the class for the BACDF (Base Array Collection Data-Frame),
this wraps multiple distributed and united ndarrays as well as
accompanying metadata. DTDF is build on this class
"""
import pandas as pd
import numpy as np


class BACDF(object):
    """
    class for base array collection data-frame

    Parameters
    ----------
    data : dict-like
        each element corresponds to an ndarray
    data_caxes : dict-like
        each element corresponds to a tuple, this tuples
        corresponds to the collection axes on which the
        corresponding data ndarray exists
    metadata : None or tuple
        see attributes
    chunks : None or tuple
        see attributes

    Attributes
    ----------
    data : dict-like
        each element corresponds to an ndarray
    caxes_caxes_map : pandas DataFrame
        indicator matrix corresponding to links between
        collection axes.
    caxes_data_map : pandas DataFrame
        indicator matrix where columns correspond to
        data labels, and rows correspond to collection
        axes.  If true, the corresponding column exists
        on the corresponding collection axis.
    caxes_daxes_map : pandas DataFrame
        columns correspond to data labels and rows correspond
        to collection axes.  The values represent the underlying
        data axis for each collection axis, data label pair.
    metadata : tuple
        tuple of metadata, each element should be a pandas
        DataFrame or None.  Should be length ndim
    ndim : scalar
        number of collection axes
    ddim : scalar
        number of data sets
    dlabels : tuple
        tuple of data labels
    shape : tuple
        length of each collection axis, should be ndim length
    chunks : tuple
        size of blocks, should be ndim length, if len(chunks[i]) > 1
        that collection axis is assumed to be distributed (dask array),
        each chunks[i] should be a tuple itself
    data_caxes : dict-like
        data_caxes used to build BACDF, kept incase we want to
        update data values
    """

    def __init__(self, data, data_caxes, metadata=None, chunks=None):

        self = self.__add_data(data, data_caxes, metadata, chunks)


    def __add_data(self, data, data_caxes, metadata, chunks):
        """adds data to a BACDF.  NOTE, this is a private method,
        it is called by __init__ as well as any methods which update
        the internal data

        Parameters
        ----------
        data : dict-like
            each element corresponds to an ndarray
        data_caxes : dict-like
            each element corresponds to a tuple, this tuples
            corresponds to the collection axes on which the
            corresponding data ndarray exists
        metadata : None or tuple
            see attributes
        chunks : None or tuple
            see attributes

        Returns
        -------
        updated version of BACDF
        """

        # prep ndim and metadata
        ndim = max([max(data_caxes[k]) for k in data_caxes]) + 1

        if metadata is None:
            metadata = tuple(None for i in range(ndim))

        if ndim != len(metadata):
            raise ValueError("metadata length does not match ndim")

        # prep ddim dlabels
        dlabels = tuple(data.keys())
        ddim = len(dlabels)

        # check data and data_caxes align
        if len(data) != len(data_caxes):
            raise ValueError("data and data_caxes must be same size")

        d_k_set = set(data.keys())
        a_k_set = set(data_caxes.keys())
        if d_k_set != a_k_set:
            raise ValueError("data and data_caxes dont share key set")

        # set up shape and axis maps
        shape = {}
        caxes_daxes_map = np.empty((ndim, ddim))
        caxes_daxes_map[:] = np.nan
        caxes_data_map = np.zeros((ndim, ddim))
        for i, k in enumerate(d_k_set):
            t_data_s = data[k].shape
            t_data_d = [i for i in range(len(t_data_s))]
            t_caxes = data_caxes[k]
            if len(t_data_s) != len(t_caxes):
                raise ValueError("data and data_caxes %s dont share dim" % k)
            else:
                caxes_daxes_map[t_caxes,i] = t_data_d
                caxes_data_map[t_caxes,i] = 1
                for s, a in zip(t_data_s, t_caxes):
                    if a not in shape:
                        shape[a] = s
                    elif shape[a] != s:
                        raise ValueError("data shapes dont align for %s" % k)
        shape = tuple(shape[k] for k in range(len(shape)))

        caxes_caxes_map = np.eye(ndim)
        for i in range(ndim):
            s_map = caxes_data_map[:,caxes_data_map[i,:] == 1]
            s_map = s_map.sum(axis=1)
            s_map = np.where(s_map > 0)[0]
            caxes_caxes_map[i,s_map] = 1
            caxes_caxes_map[s_map,i] = 1

        caxes_data_map = pd.DataFrame(caxes_data_map, columns=dlabels)
        caxes_daxes_map = pd.DataFrame(caxes_daxes_map, columns=dlabels)
        caxes_caxes_map = pd.DataFrame(caxes_caxes_map)

        # set up chunks
        if chunks is None:
            chunks = tuple((s,) for s in shape)

        if len(chunks) != len(shape):
            raise ValueError("chunks must be same length as shape")

        self.data = data
        self.caxes_caxes_map = caxes_caxes_map
        self.caxes_data_map = caxes_data_map
        self.caxes_daxes_map = caxes_daxes_map
        self.metadata = metadata
        self.ndim = ndim
        self.ddim = ddim
        self.dlabels = dlabels
        self.shape = shape
        self.chunks = chunks
        self.data_caxes = data_caxes
        return self


    def update_data(data, data_caxes, metadata=None, chunks=None):
        """links additional arrays to the collection.  Returns
        an updated version of the BACDF.  NOTE that when updating
        metadata or chunks we assume that these are ordered
        by the new axes.

        Parameters
        ----------
        data : dict-like
            each element corresponds to an ndarray
        data_caxes : dict-like
            each element corresponds to a tuple, this tuples
            corresponds to the collection axes on which the
            corresponding data ndarray exists
        metadata : None or tuple
            see attributes
        chunks : None or tuple
            see attributes

        Returns
        -------
        updated version of BACDF
        """

        # TODO cut down on duplicate operations

        # confirms that new axes are added
        ndim = max([max(data_caxes[k]) for k in data_caxes]) + 1
        if not ndim > self.ndim:
            raise ValueError("New axes must be added as part of update")

        # check data and data_caxes align
        if len(data) != len(data_caxes):
            raise ValueError("data and data_caxes must be same size")

        d_k_set = set(data.keys())
        a_k_set = set(data_caxes.keys())
        if d_k_set != a_k_set:
            raise ValueError("data and data_caxes dont share key set")

        # update metadata
        if metadata is None:
            metadata = tuple(None for i in range(ndim - self.ndim))
        metadata = self.metadata + metadata

        if ndim != len(metadata):
            raise ValueError("metadata length does not match ndim")

        # update chunks, we only need to get the shape of the new
        # data if the chunks aren't provided
        if chunks is None:

            shape = {}
            for i, k in enumerate(d_k_set):
                t_data_s = data[k].shape
                t_caxes = data_caxes[k]
                if len(t_data_s) != len(t_caxes):
                    raise ValueError("data and data_caxes %s dont share dim" % k)
                else:
                    for s, a in zip(t_data_s, t_caxes):
                        if a not in shape:
                            shape[a] = s
                        elif shape[a] != s:
                            raise ValueError("data shapes dont align for %s" % k)
            shape = tuple(shape[k] for k in range(len(shape)))
            chunks = tuple((s,) for s in shape)

        chunks = self.chunks + chunks

        # update data and data_caxes to combine new and old data
        ndata = self.data
        ndata.update(data)

        ndata_caxes = self.data_caxes
        ndata_caxes.update(data_caxes)

        return self.__add_data(ndata, ndata_caxes, metadata, chunks)


    def update_metadata(self, new_metadata, metadata_axes, overwrite=False):
        """updates the metadata with the new_metadata entries

        Parameters
        ----------
        new_metadata : iterable
            additional metadata to add
        metadata_axes : iterable
            corresponding axes for each metadata entry, each element should
            be a scalar
        overwrite : bool or scalar
            indicator for whether to overwrite current metadata if not
            None

        Returns
        -------
        updated BACDF
        """

        metadata = self.metadata
        axes = tuple(i for i in range(self.ndim))
        nmetadata = []
        for a in axes:
            if a in metadata_axes:
                if metadata[a] is None or overwrite:
                    nmetadata.append(new_metadata[a])
                else:
                    raise ValueError("can't replace non-None metadata")
            else:
                nmetadata.append(metadata[a])
        self.metadata = tuple(nmetadata)
        return self


    def update_from_pandas(self):

        return NotImplementedError


    def to_pandas(self):

        return NotImplementedError


    def rechunk(self):

        return NotImplementedError


    def update_from_csv(self):

        return NotImplementedError


    def to_csv(self):

        return NotImplementedError


    def agg(self):

        return NotImplementedError


    def in_place(self):

        return NotImplementedError


    def bool(self):

        return NotImplementedError


    def apply(self):

        return NotImplementedError
