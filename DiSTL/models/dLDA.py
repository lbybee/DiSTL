from labbot.components import Coordinator
from LDA_c_methods import LDA_pass
from datetime import datetime
import pandas as pd
import numpy as np
import sys
import os

##############################################################################
#                           Control/main functions                           #
##############################################################################

def dLDA(DTM_dir, out_dir, K, niters=500, alpha=1., beta=1., **kwds):
    """fits a distributed instance of latent dirichlet allocation (LDA)

    Parameters
    ----------
    DTM_dir : str
        location where document term matrix is located, should be formatted
        according to DiSTL DTM format
    out_dir : str
        location where topic model will be written
    K : scalar
        number of topics to estimate
    niters : scalar
        number of iterations for Gibbs samplers
    alpha : scalar
        prior for theta
    beta : scalar
        prior for beta
    kwds : dict
        additional key-words to provide for backend coordinator
    """

    # set default logging formats for various passes
    est_postfmt=("{{t0}} {{func}}        {{mod.label}} "
                 "est runtime: {{tdiff}} total runtime: {{tot_tdiff}} "
                 "ztrace: {{mod.z_trace|last}} "
                 "iter: {{mod.z_trace|length}}")

    # init coordinator
    coord = Coordinator(**kwds)

    # load DTM metadata/info
    D, V, count_fl = prep_DTM_info(DTM_dir)

    # init model
    mod_l = coord.map(init_model, count_fl, K=K, V=V,
                      alpha=alpha, beta=beta)

    # create initial nw/nwsum
    nw = np.zeros((K, V), dtype=np.intc)
    nwsum = np.zeros(K, dtype=np.intc)
    nw_l = coord.map(extract_nw, mod_l, gather=True)
    nw, nwsum = aggregate_nw(nw_l, nw, nwsum)

    # scatter global nw/nwsum
    nw_f = coord.scatter(nw, broadcast=True)
    nwsum_f = coord.scatter(nwsum, broadcast=True)

    # readd global nw/nwsum to model nodes
    mod_l = coord.map(readd_nw, mod_l, nw=nw_f, nwsum=nwsum_f)

    # fit iterations
    for s in range(niters):

        # estimate model state for current iteration
        mod_l = coord.map(est_LDA_pass, mod_l, pure=False,
                          postfmt=est_postfmt)

        # update global nw/nwsum
        nw_l = coord.map(extract_nw, mod_l, gather=True)
        nw, nwsum = aggregate_nw(nw_l, nw, nwsum)

        # scatter global nw/nwsum
        nw_f = coord.scatter(nw, broadcast=True)
        nwsum_f = coord.scatter(nwsum, broadcast=True)

        # readd global nw/nwsum to model nodes
        mod_l = coord.map(readd_nw, mod_l, nw=nw_f, nwsum=nwsum_f)

    # add theta estimates to mod state and write node output
    mod_l = coord.map(calc_post_theta, mod_l)
    coord.map(write_mod_csv, mod_l, out_dir=out_dir, gather=True)

    # add phi and write global output
    phi = calc_post_phi(nw, nwsum, beta)
    write_global_csv(nw, nwsum, phi, out_dir)


##############################################################################
#                           State/IO/gen functions                           #
##############################################################################

def prep_DTM_info(DTM_dir):
    """extracts the DTM dimensions (D, V) as well as a list of count files

    Parameters
    ----------
    DTM_dir : str
        location of DTM files

    Returns
    -------
    D : scalar
        number of documents
    V : scalar
        number of terms
    count_fl : list
        list of files corresponding to counts (one for each node)
    """

    # get dimensions
    D_l = [len(open(os.path.join(DTM_dir, d), "r").readlines())
           for d in os.listdir(DTM_dir) if d[:3] == "doc"]
    D = sum(D_l) - len(D_l)
    V_l = [len(open(os.path.join(DTM_dir, v), "r").readlines())
           for v in os.listdir(DTM_dir) if v[:4] == "term"]
    V = sum(V_l) - len(V_l)

    # count files
    count_fl = [os.path.join(DTM_dir, f) for f in os.listdir(DTM_dir)
                if f[:5] == "count"]

    return D, V, count_fl


def init_model(DTM_shard_fname, K, V, alpha, beta):
    """loads a count chunk onto a specified client node

    Parameters
    ----------
    DTM_shard_fname : str
        file-name for corresponding DTM count file
    K : scalar
        global topic count
    V : scalar
        global term count
    alpha : scalar
        prior to theta (topic loadings)
    beta : scalar
        prior for phi (topic components)

    Returns
    -------
    mod : dict
        populated dictionary with model state for the corresponding node

    Notes
    -----
    The model dict consists of the following values to start:

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
    label : str
        label for current node
    """

    # prep model dict
    mod = {}

    # set global values
    mod["K"] = K
    mod["V"] = V
    mod["alpha"] = alpha
    mod["beta"] = beta

    # extract label
    label = os.path.basename(DTM_shard_fname)
    mod["label"] = label.replace(".csv", "").replace("count_", "")

    # prep count
    count = pd.read_csv(DTM_shard_fname).values
    doc_id = count[:,0]
    doc_id -= doc_id.min()
    count[:,0] = doc_id
    count = np.array(count, dtype=np.intc)
    mod["count"] = count

    # prep node-specific dimensions
    D = np.max(doc_id) + 1
    NZ = count.shape[0]
    mod["D"] = D
    mod["NZ"] = NZ

    # init z
    z = np.random.randint(0, high=K, size=NZ, dtype=np.intc)
    mod["z"] = z

    # prep z_trace (will get built up during iterations)
    mod["z_trace"] = np.array([])

    # TODO the data-frame approach here is somewhat hacky
    # (can't we just do a similar groupby in numpy?)

    # generate nd/ndsum
    dzdf = pd.DataFrame({"d": count[:,0], "z": z, "count": count[:,2]})
    dzarr = dzdf.groupby(["d", "z"], as_index=False).sum().values
    nd = np.zeros(shape=(D, K))
    nd[dzarr[:,0], dzarr[:,1]] = dzarr[:,2]
    ndsum = nd.sum(axis=1)
    mod["nd"] = np.array(nd, dtype=np.intc)
    mod["ndsum"] = np.array(ndsum, dtype=np.intc)

    # generate nw/nwsum
    vzdf = pd.DataFrame({"v": count[:,1], "z": z, "count": count[:,2]})
    vzarr = vzdf.groupby(["z", "v"], as_index=False).sum().values
    nw = np.zeros(shape=(K, V))
    nw[vzarr[:,0], vzarr[:,1]] = vzarr[:,2]
    nwsum = nw.sum(axis=1)
    mod["nw"] = np.array(nw, dtype=np.intc)
    mod["nwsum"] = np.array(nwsum, dtype=np.intc)

    return mod


def tmp_write_nw(mod, out_dir, s):

    np.savetxt(os.path.join(out_dir, "nw_%s_%d.csv" % (mod["label"], s)),
               mod["nw"], delimiter=",")


def write_mod_csv(mod, out_dir):
    """writes all the model estimates to the specified out_dir

    Parameters
    ----------
    mod : dict
        model state for current node
    out_dir : str
        location where output will be written
    """

    var_l = ["z", "nd", "ndsum", "theta", "z_trace"]
    for var in var_l:
        np.savetxt(os.path.join(out_dir, "%s_%s.csv" % (var, mod["label"])),
                   mod[var], delimiter=",")


def write_global_csv(nw, nwsum, phi, out_dir):
    """writes the global estimates to the specified out_dir

    Parameters
    ----------
    nw : numpy array
        global values for nw
    nwsum : numpy array
        global values for nwsum
    phi : numpy array
        global values for phi
    out_dir : str
        location where output will be written
    """

    np.savetxt(os.path.join(out_dir, "nw.csv"), nw, delimiter=",")
    np.savetxt(os.path.join(out_dir, "nwsum.csv"), nwsum, delimiter=",")
    np.savetxt(os.path.join(out_dir, "phi.csv"), phi, delimiter=",")


##############################################################################
#                            Pure/calc functions                             #
##############################################################################

def est_LDA_pass(mod):
    """wrapper around the cython LDA_pass code to manage mod dict

    Parameters
    ----------
    mod : dict
        dictionary corresponding to model state for node

    Returns
    -------
    mod : dict
        updated dictionary corresponding to new model state for node

    Notes
    -----
    The cython code in LDA_pass updates the model values in-place, this
    is the reason we don't need to return anything from LDA_pass
    """

    z_prev = mod["z"].copy()

    LDA_pass(**mod)

    mod["z_trace"] = np.append(mod["z_trace"], np.sum(mod["z"] != z_prev))

    return mod


def calc_post_theta(mod):
    """calculates the posterior estimates for theta for current node

    Parameters
    ----------
    mod : dict
        dictionary containing current model state without posterior estimates

    Returns
    -------
    mod : dict
        containing additional posterior estimates for theta

    Notes
    -----
    This adds the following elements to the model dict:

    theta : numpy array (D x K)
        current estimates for document-topic proportions
    """

    mod["theta"] = ((np.array(mod["nd"]).T + mod["alpha"]) /
                    (np.array(mod["ndsum"]) + mod["K"] * mod["alpha"])).T

    return mod


def calc_post_phi(nw, nwsum, beta):
    """calculates the posterior estimate for phi

    Parameters
    ----------
    nw : numpy array
        current global nw matrix
    nwsum : numpy array
        current global nwsum vector
    beta : scalar
        prior for phi

    Returns
    -------
    phi : numpy array
        global estimates for topic-term proportions
    """

    K, V = nw.shape

    phi = ((nw.T + beta) / (nwsum + V * beta)).T

    return phi


def extract_nw(mod):
    """extracts tuple of node specific nw/nwsum"""

    return mod["nw"], mod["nwsum"]


def readd_nw(mod, nw, nwsum):
    """updates the provided model state to reflect global nw/nwsum"""

    mod["nw"] = np.array(nw, dtype=np.intc)
    mod["nwsum"] = np.array(nwsum, dtype=np.intc)

    return mod


def aggregate_nw(nw_l, nw, nwsum):
    """combines list of node specific nw/nwum estimates into global estimates

    Parameters
    ----------
    nw_l : list
        list of tuples containing node specific nw/nwsum
    nw : numpy array
        current global nw
    nwsum : numpy array
        current global nwsum

    Returns
    -------
    tuple
        updated nw/nwsum
    """

    n_nw, n_nwsum = zip(*nw_l)
    n_nw = np.sum(n_nw, axis=0, dtype=np.intc)
    n_nwsum = np.sum(n_nwsum, axis=0, dtype=np.intc)
    part = len(nw_l)

    nw = (1 - part) * nw + n_nw
    nwsum = (1 - part) * nwsum + n_nwsum

    return nw, nwsum
