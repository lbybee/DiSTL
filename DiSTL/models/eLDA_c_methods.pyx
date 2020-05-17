from libc.stdlib cimport rand, RAND_MAX
import numpy as np
cimport numpy as np
cimport cython


cdef int z_sample(int[:] nd_dv, int ndsum_dv, int[:] nw_dv, int[:] nwsum,
                  int count_dv, int K, double Kalpha, double Vbeta,
                  double alpha, double beta, double[:] p) nogil:
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


def LDA_pass(int[:,:] count, int[:] z, int[:,:] nd, int[:] ndsum,
             int[:,:] nw, int[:] nwsum, int NZ, int D, int V, int K,
             double alpha, double beta, **kwds):
    """runs one pass of LDA over the provided text count

    Parameters
    ----------
    count : numpy array (NZ x 3)
        triplet representation for term counts for current node
    z : numpy array (NZ x 1)
        topic assignments for each term
    nd : numpy array (D x K)
        weighted (by term count) topic assigments for each document
    ndsum : numpy array (D x 1)
        term counts in document d
    nw : numpy array (K x V)
        weighted (by term count) topic assigments for each term
    nwsum : numpy array (K x 1)
        total term counts assigned to topic K
    z_trace : numpy array (0:niters)
        contains the diff for z at each iteration
    NZ : scalar
        number of non-zero elements in counts
    D : scalar
        number of documents
    V : scalar
        number of terms
    K : scalar
        number of topics
    alpha : scalar
        prior for theta
    beta : scalar
        prior for phi
    """

    cdef int dv, d, v, count_dv, topic, k

    cdef double Kalpha = K * alpha
    cdef double Vbeta = V * beta

    cdef double[:] p = np.zeros(K)

    for dv in range(NZ):

        d = count[dv,0]
        v = count[dv,1]
        count_dv = count[dv,2]

        topic = z[dv]

        nd[d,topic] -= count_dv
        ndsum[d] -= count_dv
        nw[topic,v] -= count_dv
        nwsum[topic] -= count_dv

        topic = z_sample(nd[d,:], ndsum[d], nw[:,v], nwsum, count_dv, K,
                         Kalpha, Vbeta, alpha, beta, p)

        nd[d,topic] += count_dv
        ndsum[d] += count_dv
        nw[topic,v] += count_dv
        nwsum[topic] += count_dv

        z[dv] = topic
