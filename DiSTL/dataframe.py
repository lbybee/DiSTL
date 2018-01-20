"""
Includes the class for the BACDF (Base Array Collection Data-Frame),
this wraps multiple distributed and united ndarrays as well as
accompanying metadata. DTDF is build on this class
"""
import dask.dataframe as dd
import dask.array as da
import pandas as pd
import numpy as np

def _to_indicator(data_axes_map):
    """converts the dictionary representation
    of a data_axes_map to the indicator matrix
    representation."""

    ddim = len(data_axes_map)
    ndim = max([max(data_axes_map[k]) for k in data_axes_map]) + 1
    ndata_axes_map = pd.DataFrame(np.zeros((ndim, ddim)),
                                  columns=data_axes_map.keys())
    for col in data_axes_map:
        ndata_axes_map.loc[data_axes_map[col],col] = 1
    return ndata_axes_map

def _to_dict(data_axes_map):
    """converts the indicator matrix representation
    of a data_axes_map to the dictionary representation."""

    ndata_axes_map = {}
    for col in data_axes_map.columns:
        axes = data_axes_map[col]
        axes = axes[axes == 1]
        ndata_axes_map[col] = axes.index.tolist()
    return ndata_axes_map

def _metadata_chunk_map(metadata_i):
    """a function that can be applied to the series
    representation of metadata to generate chunks."""

    return tuple(metadata_i.map_partitions(len).compute())


class BACDF(object):
    """
    class for base array collection data-frame

    Parameters
    ----------
    data : dict
        dictionary mapping data labels to ndarrays,
        the data keys correspond to the columns in
        data_axes_map.  If an element of data is not
        a dask array, it will be converted to one
        with only one chunk.
    data_axes_map : dict
        dictionary mapping data labels to corresponding
        axes.  Each element (which corresponds to a
        data set/label) should be a list/tuple containing
        a scalar corresponding to a collection axis.
    metadata : dict
        dictionary mapping metadata labels to DataFrames,
        the metadata keys correspond to the rows in
        data_axes_map. If an element of metadata is not a
        dask DataFrame it will be converted to one with
        only one partition.

    Attributes
    ----------
    data : pandas Series
        series where each element corresponds to a
        data dask array.  The index corresponds to
        the columns of data_axes_map.
    data_axes_map : pandas DataFrame
        indicator matrix where columns correspond to
        data labels, and rows correspond to collection
        axes.  If true, the corresponding column exists
        on the corresponding collection axis.
    metadata : pandas Series
        series where each element corresponds to
        a metadata dask DataFrame.  The index corresponds
        to the rows of data_axes_map.
    """

    def __init__(self, data, data_axes_map, metadata):

        # data
        if not isinstance(data, dict):
            raise ValueError("Non-dict data currently not supported")
        data = {k: da.from_array(data[k], chunks=data[k].shape)
                if not isinstance(data[k], da.Array)
                else data[k] for k in data}
        data = pd.Series(data)

        # data_axes_map
        if not isinstance(data_axes_map, dict):
            raise ValueError("Non-dict data_axes_map currently not supported")
        data_axes_map = _to_indicator(data_axes_map)

        # metadata
        if not isinstance(metadata, dict):
            raise ValueError("Non-dict metadata currently not supported")
        metadata = {k: dd.from_pandas(metadata[k], npartitions=1)
                    if not isinstance(metadata[k], dd.DataFrame)
                    else metadata[k] for k in metadata}
        metadata = pd.Series(metadata)

        # confirm that dimensions align
        if len(data) != len(data_axes_map.columns):
            raise ValueError("data does not align with data_axes_map")
        if len(metadata) != len(data_axes_map.index):
            raise ValueError("metadata does not align with data_axes_map")

        self = self.__consistency_check(data, data_axes_map, metadata)


    def __consistency_check(self, data, data_axes_map, metadata):
        """determines that all the data/metadata aligns before
        returning a BACDF.  If there is information out of sync
        raises errors explaining where the problem lies.

        Parameters
        ----------
        data : pandas Series
            series where each element corresponds to a
            data dask array.  The index corresponds to
            the columns of data_axes_map.
        data_axes_map : pandas DataFrame
            indicator matrix where columns correspond to
            data labels, and rows correspond to collection
            axes.  If true, the corresponding column exists
            on the corresponding collection axis.
        metadata : pandas Series
            series where each element corresponds to
            a metadata dask DataFrame.  The index corresponds
            to the rows of data_axes_map.

        Returns
        -------
        BACDF
        """

        data_chunks = data.apply(lambda x: x.chunks)
        metadata_chunks = metadata.apply(_metadata_chunk_map)

        failure_mat = data_axes_map.copy()
        failure_mat[:] = 0
        for k in data_chunks.index:
            caxes = data_axes_map[k]
            caxes = caxes[caxes > 0].index
            for chunk_d, caxis in zip(data_chunks[k], caxes):
                if chunk_d != metadata_chunks[caxis]:
                    failure_mat[caxis,k] = 1

        if failure_mat.sum().sum() != 0:
            error_msg = "Not all data and metadata chunks align\n\n"
            for k in failure_mat.columns:
                caxes = failure_map[k]
                caxes = caxes[caxes > 0].index
                for a in caxes:
                    error_msg += "%s and %d chunks don't align\n" % (k, a)
            raise ValueError(error_msg)

        else:
            self.data = data
            self.metadata = metadata
            self.data_axes_map = data_axes_map
            return self


    def update(self, data=None, data_axes_map=None, metadata=None):
        """updates a BACDF with the provided values

        Parameters
        ----------
        data : dict-like
            dictionary mapping data labels to ndarrays,
            the data keys correspond to the columns in
            data_axes_map.
        data_axes_map : pandas DataFrame
            indicator matrix where columns correspond to
            data labels, and rows correspond to collection
            axes.  If true, the corresponding column exists
            on the corresponding collection axis.
        metadata : dict
            dictionary mapping metadata labels to DataFrames,
            the metadata keys correspond to the rows in
            data_axes_map. If an element of metadata is not a
            dask DataFrame it will be converted to one with
            only one partition.

        Returns
        -------
        updated BACDF
        """

        if data is None:
            data = {}
        if data_axes_map is None:
            data_axes_map = {}
        if metadata is None:
            metadata = {}

        # data
        if not isinstance(data, dict):
            raise ValueError("Non-dict data currently not supported")
        ndata = self.data.to_dict()
        ndata.update(data)
        ndata = {k: da.from_array(ndata[k], chunks=ndata[k].shape)
                 if not isinstance(ndata[k], da.Array)
                 else ndata[k] for k in ndata}
        ndata = pd.Series(ndata)

        # data_axes_map
        if not isinstance(data_axes_map, dict):
            raise ValueError("Non-dict data_axes_map currently not supported")
        ndata_axes_map = _to_dict(self.data_axes_map)
        ndata_axes_map.update(data_axes_map)
        ndata_axes_map = _to_indicator(ndata_axes_map)

        # metadata
        if not isinstance(metadata, dict):
            raise ValueError("Non-dict metadata currently not supported")
        nmetadata = self.metadata.to_dict()
        nmetadata.update(metadata)
        nmetadata = {k: dd.from_pandas(nmetadata[k], npartitions=1)
                     if not isinstance(nmetadata[k], dd.DataFrame)
                     else nmetadata[k] for k in nmetadata}
        nmetadata = pd.Series(nmetadata)

        # confirm that dimensions align
        if len(ndata) != len(ndata_axes_map.columns):
            raise ValueError("data does not align with data_axes_map")
        if len(nmetadata) != len(ndata_axes_map.index):
            raise ValueError("metadata does not align with data_axes_map")

        return self.__consistency_check(ndata, ndata_axes_map, nmetadata)


    @property
    def ndim(self):

        return len(self.metadata)

    @property
    def ddim(self):

        return len(self.data)

    @property
    def shape(self):

        return tuple(self.metadata.apply(len))

    @property
    def chunks(self):

        return tuple(self.metadata.apply(_metadata_chunk_map))


    def update_from_pandas(self):

        return NotImplementedError


    def to_pandas(self):

        return NotImplementedError


    def rechunk(self, axis=0):

        return NotImplementedError


    def update_from_csv(self):

        return NotImplementedError


    def to_csv(self):

        return NotImplementedError


    def sum(self, axis, label=None):
        """returns an updated version of the BACDF summed
        over the given axis, if label is given this operation
        is only applied to the labeled array, and the axes
        info is updated to represent the change

        Parameters
        ----------
        axis : scalar
            axis over which to sum
        label : str
            data key for array

        Returns
        -------
        summed BACDF
        """

        if label is None:
            label = self.data.index
        else:
            label = [label]

        self.data[label] = self.data[label].apply(lambda x: x.sum(axis=axis))

        # build labels and underlying daxes to iterate over
        # and sum
        if label is None:
            data_map = self.caxes_data_map.iloc[axis,:]
            daxes_map = self.caxes_daxes_map.iloc[axis,:]
            ind = data_map == 1
            data_map = data_map[ind]
            daxes_map = daxes_map[ind]
            labels = data_map.index.values
            daxes = daxes_map.values
        else:
            labels = [label]
            daxes = [self.caxes_daxes_map.loc[axis,label]]

        # apply sum operation
        for l, da in zip(labels, daxes):
            self.data[l] = self.data[l].sum(axis=da)
            self.caxes_data_map.loc[axis,l] = 0
            self.caxes_daxes_map.loc[axis,l] = np.nan

        return self


    def agg(self):

        return NotImplementedError


    def in_place(self):

        return NotImplementedError


    def bool(self):

        return NotImplementedError


    def apply(self):

        return NotImplementedError
