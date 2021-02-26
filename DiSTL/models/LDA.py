from LDA_c_methods import LDA_pass, eLDA_pass, eLDA_pass_b
from coordinator import Coordinator
from datetime import datetime
import pandas as pd
import numpy as np
import glob
import sys
import os

# TODO currently only base LDA supports aggregate values

##############################################################################
#                           Control/main functions                           #
##############################################################################

def LDA(DTM_dir, out_dir, K, niters=500, alpha=1., beta=1.,
        LDA_method="efficient_b", fin_agg_iter=0, **kwds):
    """fits a sequential instance of latent dirichlet allocation (LDA)

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
    LDA_method : str
        type of LDA method used for estimation

        full : take gibbs sample for every term

        efficient : take gibbs sample for every unique term

    fin_agg_iter : scalar
        number of iterations at end of path to aggregate
    kwds : dict
        additional key-words to provide for backend coordinator
    """

    # load DTM metadata/info
    D, V, count_fl = prep_DTM_info(DTM_dir)

    # init model
    mod_l = [init_model(f, K=K, V=V, alpha=alpha, beta=beta,
                        LDA_method=LDA_method, fin_agg_iter=fin_agg_iter)
             for f in count_fl]
    if len(mod_l) > 1:
        mod = aggregate_mod(mod_l)
    else:
        mod = mod_l[0]

    # fit iterations
    for s in range(niters):

        # calc finagg
        if fin_agg_iter > 0:
            finagg = (s >= (niters - fin_agg_iter))
        else:
            finagg = False

        # estimate model state for current iteration
        mod = est_LDA_pass(mod, LDA_method=LDA_method, finagg=finagg)
        msg = est_LDA_logger(mod)
        with open("log.txt", "a") as fd:
            fd.write(msg + "\n")

    # add theta estimates to mod state and write node output
    write_mod_csv(mod, out_dir)

    # add phi and write global output
    write_global_csv(mod["nw"], out_dir, "nw")
    if "nwfinagg" in mod:
        write_global_csv(mod["nwfinagg"], out_dir, "nwfinagg")


def oLDA(DTM_dir, out_dir, K, niters=500, alpha=1., beta=1.,
          LDA_method="efficient_b", omega=1., **kwds):
    """fits a online instance of latent dirichlet allocation (LDA)

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
        starting prior for beta
    LDA_method : str
        type of LDA method used for estimation

        full : take gibbs sample for every term

        efficient : take gibbs sample for every unique term
    omega : scalar
        weight value for using previous phi fits as prior

    kwds : dict
        additional key-words to provide for backend coordinator

    Notes
    -----
    This is essentially an exponentially smoothed TM
    """

    # load DTM metadata/info
    D, V, count_fl = prep_DTM_info(DTM_dir)

    # prep list to hold prior term counts
    nw_l = []

    # iteratively fit models
    for f in count_fl:

        # load model
        mod = init_model(f, K=K, V=V, alpha=alpha, beta=beta,
                         LDA_method=LDA_method, nw_l=nw_l,
                         omega=omega)

        # fit online part
        for s in range(niters):

            # estimate model state for current iteration
            mod = est_LDA_pass(mod, LDA_method=LDA_method)
            msg = est_LDA_logger(mod)
            with open("log.txt", "a") as fd:
                fd.write(msg + "\n")

        # add theta estimates to mod state and write node output
        write_mod_csv(mod, out_dir)

        # add phi and write global output
        write_online_global_csv(mod["label"], mod["nw"], out_dir)

        # add to nw l
        nw_l.append(mod["nw"])



def dLDA(DTM_dir, out_dir, K, niters=500, alpha=1., beta=1.,
         LDA_method="efficient_b", **kwds):
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
    LDA_method : str
        type of LDA method used for estimation

        full : take gibbs sample for every term

        efficient : take gibbs sample for every unique term

    kwds : dict
        additional key-words to provide for backend coordinator
    """

    # init coordinator
    coord = Coordinator(**kwds)

    # load DTM metadata/info
    D, V, count_fl = prep_DTM_info(DTM_dir)

    # init model
    mod_l = coord.map(init_model, count_fl, K=K, V=V,
                      alpha=alpha, beta=beta, LDA_method=LDA_method)

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
        mod_l = coord.map(est_LDA_pass, mod_l, LDA_method=LDA_method,
                          pure=False, log=True, func_logger=est_LDA_logger)

        # update global nw/nwsum
        nw_l = coord.map(extract_nw, mod_l, gather=True)
        nw, nwsum = aggregate_nw(nw_l, nw, nwsum)

        # scatter global nw/nwsum
        nw_f = coord.scatter(nw, broadcast=True)
        nwsum_f = coord.scatter(nwsum, broadcast=True)

        # readd global nw/nwsum to model nodes
        mod_l = coord.map(readd_nw, mod_l, nw=nw_f, nwsum=nwsum_f)

    # write node output
    coord.map(write_mod_csv, mod_l, out_dir=out_dir, gather=True)

    # add phi and write global output
    write_global_csv(nw, out_dir, "nw")


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
    # TODO handle doc/term more cleanly
    D_l = [len(open(os.path.join(DTM_dir, d), "r").readlines())
           for d in os.listdir(DTM_dir) if d[:3] == "doc"]
    D = sum(D_l) - len(D_l)
    V_l = [len(open(os.path.join(DTM_dir, v), "r").readlines())
           for v in os.listdir(DTM_dir) if v[:4] == "term"]
    V = sum(V_l) - len(V_l)

    # count files
    count_fl = sorted(glob.glob(os.path.join(DTM_dir, "count*.csv")))

    return D, V, count_fl


def init_model(DTM_shard_fname, K, V, alpha, beta, LDA_method,
               nw_l=None, omega=None, fin_agg_iter=0):
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
    LDA_method : str
        type of LDA method used for estimation
    nw_l : list or None
        list of previously fit phi count matrices
    omega : scalar or None
        if provided this corresponds to weight for prior data
    fin_agg_iter : scalar
        number of iterations from end of path to aggregate (sum) up

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

    # TODO currently the full method will fail in cases where the z
    # doesn't align with our nd/nw construction, need to fix this

    # prep model dict
    mod = {}

    # set global values
    mod["K"] = K
    mod["V"] = V
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

    # handle nw based prior
    if LDA_method != "efficient_b":
        if nw_l is not None:
            N_m = mod["count"][:,2].sum()
            m_i = len(nw_l)
            beta_m = np.ones((K, V)) * beta
            for bstp in range(m_i):
                weight = omega ** (bstp + 1)
                nw_b = nw_l[m_i-bstp-1]
                N_b = nw_b.sum()
                beta_m += (nw_b * weight / N_b) * N_m
            mod["beta"] = beta_m
            mod["betasum"] = mod["beta"].sum(axis=1)
        else:
            mod["beta"] = np.ones((K, V)) * beta
            mod["betasum"] = mod["beta"].sum(axis=1)

        # set theta prior
        mod["alpha"] = np.ones((D, K)) * alpha
        mod["alphasum"] = mod["alpha"].sum(axis=1)

    else:
        mod["beta"] = beta
        mod["alpha"] = alpha

    # init z
    # TODO currently full estimation doesn't support zprior
    # p(z_i=k|w_i) = p(w_i|z_i=k)p(z_i=k)/p(w_i)
    if LDA_method == "full":
        N = np.sum(count[:,2])
        z = np.random.randint(0, high=K, size=N, dtype=np.intc)
    elif LDA_method == "efficient":
        zprior = (mod["beta"] / mod["beta"].sum(axis=0)).T
        zprob = zprior[count[:,1]]
        u = np.random.rand(NZ, 1)
        z = (u < zprob.cumsum(axis=1)).argmax(axis=1)
        z = np.array(z, dtype=np.intc)
    elif LDA_method == "efficient_b":
        NZ = count.shape[0]
        z = np.random.randint(0, high=K, size=NZ, dtype=np.intc)
    else:
        raise ValueError("Unknown LDA_method: %s" % LDA_method)
    mod["z"] = z

    # prep z_trace (will get built up during iterations)
    mod["z_trace"] = np.array([])

    # TODO the data-frame approach here is somewhat hacky
    # (can't we just do a similar groupby in numpy?)

    # generate nd/ndsum
    if LDA_method == "full":
        dzdf = pd.DataFrame({"d": np.repeat(count[:,0], count[:,2]), "z": z})
        dzdf["count"] = 1
        dzarr = dzdf.groupby(["d", "z"], as_index=False).sum().values
    elif LDA_method == "efficient" or LDA_method == "efficient_b":
        dzdf = pd.DataFrame({"d": count[:,0], "z": z, "count": count[:,2]})
        dzarr = dzdf.groupby(["d", "z"], as_index=False).sum().values
    nd = np.zeros(shape=(D, K))
    nd[dzarr[:,0], dzarr[:,1]] = dzarr[:,2]
    ndsum = nd.sum(axis=1)
    mod["nd"] = np.array(nd, dtype=np.intc)
    mod["ndsum"] = np.array(ndsum, dtype=np.intc)

    # generate nw/nwsum
    if LDA_method == "full":
        vzdf = pd.DataFrame({"v": np.repeat(count[:,1], count[:,2]), "z": z})
        vzdf["count"] = 1
        vzarr = vzdf.groupby(["z", "v"], as_index=False).sum().values
    elif LDA_method == "efficient" or LDA_method == "efficient_b":
        vzdf = pd.DataFrame({"v": count[:,1], "z": z, "count": count[:,2]})
        vzarr = vzdf.groupby(["z", "v"], as_index=False).sum().values
    nw = np.zeros(shape=(K, V))
    nw[vzarr[:,0], vzarr[:,1]] = vzarr[:,2]
    nwsum = nw.sum(axis=1)
    mod["nw"] = np.array(nw, dtype=np.intc)
    mod["nwsum"] = np.array(nwsum, dtype=np.intc)

    # add aggregate array if desired
    if fin_agg_iter > 0:
        mod["nwfinagg"] = np.zeros(mod["nw"].shape, dtype=np.intc)
        mod["ndfinagg"] = np.zeros(mod["nd"].shape, dtype=np.intc)

    return mod


def write_mod_csv(mod, out_dir):
    """writes all the model estimates to the specified out_dir

    Parameters
    ----------
    mod : dict
        model state for current node
    out_dir : str
        location where output will be written
    """

#    var_l = ["theta", "z", "z_trace"]
    var_l = ["nd", "z", "z_trace"]
    for var in var_l:
        np.savetxt(os.path.join(out_dir, "%s_%s.csv" % (var, mod["label"])),
                   mod[var], delimiter=",")

    if "ndfinagg" in mod:
        np.savetxt(os.path.join(out_dir, "%s_%s.csv" %
                                ("ndfinagg", mod["label"])),
                   mod["ndfinagg"], delimiter=",")


def write_global_csv(nw, out_dir, lab):
    """writes the global estimates to the specified out_dir

    Parameters
    ----------
    nw : numpy array
        global values for nw
    out_dir : str
        location where output will be written
    """

    np.savetxt(os.path.join(out_dir, "%s.csv" % lab), nw, delimiter=",")


def write_online_global_csv(label, nw, out_dir):
    """writes the global estimates to the specified out_dir for oLDA

    Parameters
    ----------
    label : str
        label for current online partition
    nw : numpy array
        global values for nw
    out_dir : str
        location where output will be written
    """

    np.savetxt(os.path.join(out_dir, "nw_%s.csv" % label),
               nw, delimiter=",")


##############################################################################
#                            Pure/calc functions                             #
##############################################################################

def est_LDA_pass(mod, LDA_method="full", finagg=False):
    """wrapper around the cython LDA_pass code to manage mod dict

    Parameters
    ----------
    mod : dict
        dictionary corresponding to model state for node
    s : scalar
        current iteration
    LDA_method : str
        type of LDA method used for estimation
    finagg : bool
        whether to aggregate the current fits into finagg

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

    if LDA_method == "full":
        LDA_pass(**mod)
    elif LDA_method == "efficient":
        eLDA_pass(**mod)
    elif LDA_method == "efficient_b":
        eLDA_pass_b(**mod)
    else:
        raise ValueError("Unknown LDA_method: %s" % LDA_method)

    mod["z_trace"] = np.append(mod["z_trace"], np.sum(mod["z"] != z_prev))

    if finagg:
        mod["ndfinagg"] += mod["nd"]
        mod["nwfinagg"] += mod["nw"]

    return mod


def est_LDA_logger(mod):
    """produces a message based on current mod state for logging"""

    lab = mod["label"]
    zt = mod["z_trace"]
    t0 = datetime.now()
    msg = "label: {0} time: {1} iter: {2} trace: {3}".format(lab, t0, len(zt) - 1, zt[-1])
    return msg


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

    # TODO this and phi are foobared by vectorizing alpha/beta

    mod["theta"] = ((np.array(mod["nd"]) + mod["alpha"]).T /
                    (np.array(mod["ndsum"]) + mod["alphasum"])).T

    return mod


def calc_post_phi(nw, nwsum, beta, betasum):
    """calculates the posterior estimate for phi

    Parameters
    ----------
    nw : numpy array
        current global nw matrix
    nwsum : numpy array
        current global nwsum vector
    beta : numpy array
        matrix prior for phi
    betasum : numpy array
        sum of beta over V

    Returns
    -------
    phi : numpy array
        global estimates for topic-term proportions
    """

    K, V = nw.shape

    phi = ((nw.T + beta.T) / (nwsum + betasum)).T

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


def aggregate_mod(mod_l):
    """aggregate a list of model objects into one model object

    Parameters
    ----------
    mod_l : list
        list of dictionaries where each corresponds to a model object

    Returns
    -------
    mod : dict
        model object aggregate of list values
    """

    mod = {}

    # build shared vars
    mod["K"] = mod_l[0]["K"]
    mod["V"] = mod_l[0]["V"]
    mod["alpha"] = mod_l[0]["alpha"]
    mod["beta"] = mod_l[0]["beta"]
    mod["alphasum"] = mod_l[0]["alphasum"]
    mod["betasum"] = mod_l[0]["betasum"]
    mod["z_trace"] = mod_l[0]["z_trace"]

    # build joined vars
    mod["D"] = sum([m["D"] for m in mod_l])
    mod["NZ"] = sum([m["NZ"] for m in mod_l])

    # setup label
    # TODO is this the most sensible choice here?
    mod["label"] = mod_l[-1]["label"]

    # build count with offset
    count_l = []
    offset = 0
    for m in mod_l:
        c = m["count"]
        c[:,0] += offset
        count_l.append(c)
        offset += m["D"]
    mod["count"] = np.concatenate(count_l)
    mod["count"] = np.array(mod["count"], dtype=np.intc)

    # build doc aggs
    mod["z"] = np.concatenate([m["z"] for m in mod_l])
    mod["z"] = np.array(mod["z"], dtype=np.intc)
    mod["nd"] = np.concatenate([m["nd"] for m in mod_l])
    mod["nd"] = np.array(mod["nd"], dtype=np.intc)
    mod["ndsum"] = np.concatenate([m["ndsum"] for m in mod_l])
    mod["ndsum"] = np.array(mod["ndsum"], dtype=np.intc)

    # build term aggs
    mod["nw"] = np.array([m["nw"] for m in mod_l]).sum(axis=0)
    mod["nw"] = np.array(mod["nw"], dtype=np.intc)
    mod["nwsum"] = np.array([m["nwsum"] for m in mod_l]).sum(axis=0)
    mod["nwsum"] = np.array(mod["nwsum"], dtype=np.intc)

    return mod
