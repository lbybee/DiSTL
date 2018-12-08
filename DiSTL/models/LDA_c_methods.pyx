from libc.stdlib cimport rand, RAND_MAX
import numpy as np
cimport numpy as np


def populate(int[:,:] counts, int[:] z, int D, int V, int K, int NZ):
    """populates the zs to each aggregate"""

    cdef int[:,:] nd = np.zeros(D, K)
    cdef int[:] ndsum = np.zeros(D)
    cdef int[:,:] nw = np.zeros(K, V)
    cdef int[:] nwsum = np.zeros(K)

    cdef int dv, d, v, count_dv, topic

    for dv in range(NZ):

        d = counts[dv,0]
        v = counts[dv,1]
        count_dv = counts[dv,2]

        topic = z[dv]

        print(d, v, count_dv, topic)

        nd[d,topic] += count_dv
        ndsum[d] += count_dv
        nw[topic,v] += count_dv
        nwsum[topic] += count_dv

    return nd, ndsum, nw, nwsum


def z_sample(int[:] nd_dv, int ndsum_dv, int[:] nw_dv, int[:] nwsum,
             int count_dv, int K, double Vbeta, double Kalpha,
             double beta, double alpha, double[:] p):
    """generates a topic sample from the current estimates"""

    cdef int k, topic
    cdef double u

    p[0] = ((nw_dv[0] + beta) / (nwsum[0] + Vbeta) *
            (nd_dv[0] + alpha) / (ndsum_dv + Kalpha))

    for k in range(1, K):
        p[k] = ((nw_dv[k] + beta) / (nwsum[k] + Vbeta) *
                (nd_dv[k] + alpha) / (ndsum_dv + Kalpha))
        p[k] += p[k - 1]

    u = rand() / float(RAND_MAX) * p[K - 1]

    for topic in range(K):
        if p[topic] > u:
            break

    return topic


def LDA_pass(int[:,:] counts, int[:] z, int[:,:] nd, int[:] ndsum,
             int[:,:] nw, int[:] nwsum, int NZ, int D, int V, int K,
             double beta, double alpha):
    """runs one pass of LDA over the provided text counts

    Parameters
    ----------
    counts : NZ x 3 array
    """

    cdef int dv, d, v, count_dv, topic

    cdef double Vbeta = V * beta
    cdef double Kalpha = K * alpha
    cdef double[:] p = np.zeros(K)

    for dv in range(NZ):

        d = counts[dv,0]
        v = counts[dv,1]
        count_dv = counts[dv,2]

        topic = z[dv]

        nd[d,topic] -= count_dv
        ndsum[d] -= count_dv
        nw[topic,v] -= count_dv
        nwsum[topic] -= count_dv

        topic = z_sample(nd[d,:], ndsum[d], nw[:,v], nwsum, count_dv, K,
                         Vbeta, Kalpha, beta, alpha, p)

        nd[d,topic] += count_dv
        ndsum[d] += count_dv
        nw[topic,v] += count_dv
        nwsum[topic] += count_dv

        z[dv] = topic

    return z, nd, ndsum, nw, nwsum
