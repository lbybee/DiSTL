from LDA_c_methods import populate, LDA_pass
import dask.array as da
import pandas as pd
import numpy as np


D = 10000
V = 1000
C = 500
K = 10
NZ = 100000
part = 10
part_size = NZ / part


corpus = np.array([np.random.randint(0, high=(D-1), size=NZ, dtype=np.intc),
                   np.random.randint(0, high=(V-1), size=NZ, dtype=np.intc),
                   np.random.randint(0, high=C, size=NZ, dtype=np.intc)]).T
z = np.random.randint(0, high=(K-1), size=NZ, dtype=np.intc)
corpus = da.from_array(corpus, chunks=(part_size, 3))
z = da.from_array(z, chunks=(part_size, 1))

corpus_del = corpus.to_delayed()
z_del = z.to_delayed()


def pop_wrapper(corpus_i, z_i):
    """populates a partition"""



    nd = np.zeros((D, K), dtype=np.intc)
    ndsum = np.zeros(D, dtype=np.intc)
    nw = np.zeros((K, V), dtype=np.intc)
    nwsum = np.zeros(K, dtype=np.intc)

nd, ndsum, nw, nwsum = populate(corpus, z, nd, ndsum, nw, nwsum, NZ)

z, nd, ndsum, nw, nwsum = LDA_pass(corpus, z, nd, ndsum, nw, nwsum,
                                   NZ, D, V, K, 0.1, 0.1)
