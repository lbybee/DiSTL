from LDA_c_methods import populate, LDA_pass
import numpy as np
import dask
import os


def _populate_part(counts_i, V, K):
    """generates the necessary variables for the current partition

    Parameters
    ----------
    counts_i : pandas DataFrame
        counts for current partition (i)
    V : scalar
        number of terms in DTM
    K : scalar
        number of topics for model

    Returns
    -------
    dict of arrays
    """

    # get D_i (number of unique documents in counts_i)
    D_i = counts_i.iloc[:,0].unique().shape[0]

    # get number of non-zero counts (dim 0 for counts_i)
    NZ_i = counts_i.shape[0]

    # TODO do we need to convert to an array here? Is there a better way?
    # convert to array (for simplicity when interacting cython code)
    counts_i = counts_i.values

    # init z
    z = np.random.randint(0, high=(K - 1), size=NZ_i, dtype=np.intc)

    # populate arrays
    nd, ndsum, nw, nwsum = populate(counts_i, z, D_i, V, K, NZ_i)

    part = {"counts": counts_i, "z": z, "nd": nd, "ndsum": ndsum,
            "nw": nw, "nwsum": nwsum, "D": D_i, "NZ": NZ_i}

    return part


def _est_part(part, V, K, beta, alpha):
    """estimates the model for the current partition"""

    res = LDA_pass(V=V, K=K, beta=beta, alpha=alpha, **part)
    part["z"], part["nd"], part["ndsum"], part["nw"], part["nwsum"] = res

    return part


def _est_theta_part(part, K, alpha):
    """updates part with estimate for theta (which incorporates priors)"""

    part["theta"] = ((part["nd"].T + alpha) / (part["ndsum"] + K * alpha)).T

    return part


def _write_part(part, count_f, out_data_dir):
    """writes the results for the given partition to the specified data dir

    Parameters
    ----------
    part : dict-like
        dictionary of arrays
    fpattern : str
        pattern used to write results

    Returns
    -------
    None
    """

    for var in ["z", "nd", "theta"]:
        f = fpattern % var
        np.savetxt(f, part[var], delimiter=",")


def PLDA_method(counts, D, V, K, niters=500, alpha=None, beta=None):
    """the core method for running LDA using cython and parallel estimation

    Parameters
    ----------
    counts : dask dataframe
        the triplet representation of the counts, that is the first column
        should be doc_id the second column should be term_id and the third
        column should be count
    D : scalar
        number of documents in counts
    V : scalar
        number of terms in counts
    K : scalar
        number of topics we wish to estimate
    alpha : scalar or None
        prior for theta
    beta : scalar or None
        prior for phi

    Returns
    -------
    dictionary containing topic model estimates
    """

    # init priors
    if alpha is None:
        alpha = 1. / K
    if beta = is None:
        if V <= 100:
            beta = 1e-10
        else:
            beta = 100. / V

    # convert dask data-frame to partitions
    counts_del = counts.to_delayed()

    # init arrays
    part_l = [dask.delayed(_populate_part)(counts_i, V, K)
              for counts_i in counts_del]
    part_l = dask.compute(*part_l)

    # group nw

    # init empty values
    nw = np.zeros(K, V)
    nwsum = np.zeros(K)

    # pull data from part to group
    for part in part_l:
        nw += np.array(part["nw"])
        nwsum += np.array(part["nwsum"])

    # write grouped data back to part
    for part in part_l:
        part["nw"] = nw
        part["nwsum"] = nwsum

    for s in range(niters):

        # estimate seperate models
        part_l = [dask.delayed(_est_part)(part, V, K, beta, alpha)
                  for part in part_l]
        part_l = dask.compute(*part_l)

        # recombine nw estimates

        # init empty arrays for diffs
        nw_diff = np.zeros(K, V)
        nwsum_diff = np.zeros(K)

        # pull data from part to group
        for part in part_l:
            nw_diff = np.array(part["nw"]) - nw
            nwsum_diff = np.array(part["nwsum"]) - nwsum

        # update nw
        nw += nw_diff
        nwsum += nwsum_diff

        # write grouped data back to part
        for part in part_l:
            part["nw"] = nw
            part["nwsum"] = nwsum

    # estimate theta and phi
    part_l = [dask.delayed(_est_theta_part)(part, K, alpha)
              for part in part_l]
    part_l = dask.compute(*part_l)

    phi = (nw + beta) / (nwsum + V * beta)

    res = {"part_l": part_l, "nw": nw, "phi": phi}

    return res


def PLDA_wrapper(in_data_dir, out_data_dir, K, niters=500, alpha=None,
                 beta=None):
    """wrapper method for estimating LDA"""

    if LDA_kwds is None:
        LDA_kwds = {}

    # load data and gen vars
    doc_id = dd.read_csv(os.path.join(in_data_dir, "doc_id_*.csv"))
    term_id = dd.read_csv(os.path.join(in_data_dir, "term_id_*.csv"))

    D = len(doc_id)
    V = len(term_id)

    del doc_id
    del term_id

    # load counts
    counts = dd.read_csv(os.path.join(in_data_dir, "count_*.csv"))

    # estimate model
    res = LDA_method(counts, D, V, K, niters, alpha, beta)

    # write results
    write_l = [dask.delayed(_write_part)(part, fpattern)
               for part in res["part_l"]]
    dask.compute(*write_l)

    np.savetxt(os.path.join(out_data_dir, "nw.csv"), res["nw"],
               delimiter=",")
    np.savetxt(os.path.join(out_data_dir, "phi.csv"), res["phi"],
               delimiter=",")
