"""
Includes the class for the BDF (Base Data-Frame), this wraps
multiple distributed and united ndarrays as well as accompanying
metadata. DTDF is build on this class
"""


class BDF(object):
    """
    class for base data-frame

    Parameters
    ----------
    data : dict-like
        each element corresponds to an ndarray
    axes : dict-like
        each element corresponds to a list, this list
        corresponds to the axes on which the corresponding
        data ndarray exists
    metadata : None or list
        see attributes
    ndim : None or scalar
        see attributes
    chunks : None or tuple

    Attributes
    ----------
    data : dict-like
        each element corresponds to an ndarray
    data_axes : dict-like
        each element corresponds to a tuple, this tuple
        corresponds to the axes on which the corresponding
        data ndarray exists
    metadata : tuple
        list of metadata, each element should be a pandas
        DataFrame or None.  Should be length ndim
    ndim : scalar
        number of axes
    shape : tuple
        length of each axis, should be ndim length
    chunks : tuple
        size of blocks, should be ndim length, if len(chunks[i]) > 1
        that axis is assumed to be distributed (dask array), each
        chunks[i] should be a tuple itself
    """
    # TODO fix single length chunks

    def __init__(self, data, data_axes, metadata=None, ndim=None,
                 chunks=None):

        # prep ndim and metadata
        if ndim is None:
            ndim = max([max(data_axes[k]) for k in data_axes]) + 1

        if metadata is None:
            metadata = [None] * ndim

        if ndim != len(metadata):
            raise ValueError("metadata length does not match ndim")

        # check data and axes align
        if len(data) != len(data_axes):
            raise ValueError("data and data_axes must be same size")

        d_k_set = set(data.keys())
        a_k_set = set(data_axes.keys())
        if d_k_set != a_k_set:
            raise ValueError("data and data_axes don't share key set")

        # set up chunks and shape
        shape = {}
        for k in d_k_set:
            t_data_s, t_axes = data[k].shape, data_axes[k]
            if len(t_data_s) != len(t_axes):
                raise ValueError("data and data_axes %s don't share dim" % k)
            else:
                for s, a in zip(t_data_s, t_axes):
                    if a not in shape:
                        shape[a] = s
                    elif shape[a] != s:
                        raise ValueError("data shapes don't align for %s" % k)
        shape = tuple(shape[k] for k in range(len(shape)))

        if chunks is None:
            chunks = tuple((s,) for s in shape)

        if len(chunks) != len(shape):
            raise ValueError("chunks must be same length as shape")

        self.data = data
        self.data_axes = data_axes
        self.metadata = metadata
        self.ndim = ndim
        self.shape = shape
        self.chunks = chunks


    def agg(self):

        return None


    def in_place(self):

        return None


    def bool(self):

        return None


    def to_pandas(self):

        return None


    def apply(self):

        return None


    def to_distributed(self):

        return None


    def from_distributed(self):

        return None
