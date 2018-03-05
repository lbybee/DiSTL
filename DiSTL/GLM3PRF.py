from datetime import datetime
from dask import delayed
import statsmodels.formula.api as smf
import statsmodels.api as sm
import dask.dataframe as dd
import dask.array as da
import pandas as pd
import numpy as np
import sparse
import pickle
import patsy
import dask
import os


def _time_series_GLM(dtm, proxy, family, part=0, k=0):
    """fits a time series GLM for each column of the DTM

    Parameters
    ----------
    dtm : sparse matrix
        corresponds to document term matrix
    proxy : pandas DataFrame
        contains proxy variables
    family : statsmodels GLM family instance
        passed into GLM model to determine family
    part : scalar
        current partition
    k : scalar
        current k

    Returns
    -------
    Returns numpy array containings phi
    """

    t0 = datetime.now()
    N, p = dtm.shape

    res = []
    for i in range(p):
        t1 = datetime.now()
        resp = dtm[:,i].todense()
        model = sm.GLM(resp, proxy, family=family)
        fit = model.fit()
        # drop the intercept
        params = fit.params[1:]
        res.append(params)
        t2 = datetime.now()
        print("ts", i, part, k, t2 - t1, t2 - t0)

    return np.array(res)


def _cross_section_GLM(dtm, phi, family, part=0, k=0):
    """fits a cross section GLM for each row of the DTM

    Parameters
    ----------
    dtm : sparse matrix
        corresponds to document term matrix
    phi : pandas DataFrame
        contains term-factor loadings
    family : statsmodels GLM family instance
        passed into GLM model to determine family
    part : scalar
        current partition
    k : scalar
        current k

    Returns
    -------
    Returns numpy array containings factors
    """

    t0 = datetime.now()
    N, p = dtm.shape

    res = []
    for i in range(N):
        t1 = datetime.now()
        resp = dtm[i,:].todense()
        model = sm.GLM(resp, phi, family=family)
        fit = model.fit()
        # drop the intercept
        params = fit.params[1:]
        res.append(params)
        t2 = datetime.now()
        print("cs", i, part, k, t2 - t1, t2 - t0)

    return np.array(res)


def _time_series_pred(resp, factors):
    """runs the time series regression of the response variable
    on the latent factors.

    Parameters
    ----------
    resp : numpy series
        series corresponding to response variable
    factors : pandas DataFrame
        contains latent factors


    Returns
    -------
    statsmodels fit instance
    """

    mod = sm.OLS(resp, factors)
    fit = mod.fit()
    return fit


def GLM_3PRF(dtm, resp, K, family=sm.families.Binomial(), prior_proxy=None,
             out_dir=None):
    """returns 3PRF parameters"""

    if prior_proxy is None:
        proxy_l = [resp]
        kmin = 0
    else:
        # TODO this is inefficient
        proxy_l = [prior_proxy[r].values for r in prior_proxy.columns]
        kmin = len(proxy_l)

    proxy = pd.DataFrame(proxy_l).T

    # prep chunks and structure info for DTM
    chunks = dtm.chunks
    D, p = dtm.shape
    ts_chunks = ((D,), chunks[1])
    cs_chunks = (chunks[0], (p,))

    for k in range(kmin, K):

        # run time series regression
        ts_dtm = dtm.rechunk(ts_chunks).to_delayed().flatten()

        dmat = sm.add_constant(proxy)
        del_l = [delayed(_time_series_GLM)(dtm_i, dmat, family, part=i, k=k)
                 for i, dtm_i in enumerate(ts_dtm)]
        comp_l = dask.compute(*del_l)
        phi = da.concatenate([da.from_array(a, a.shape) for a in comp_l],
                             axis=0).compute()

        # run cross section regression
        cs_dtm = dtm.rechunk(cs_chunks).to_delayed().flatten()

        phi = pd.DataFrame(phi)
        dmat = sm.add_constant(phi)
        del_l = [delayed(_cross_section_GLM)(dtm_i, dmat, family, part=i, k=k)
                 for i, dtm_i in enumerate(cs_dtm)]
        comp_l = dask.compute(*del_l)
        factors = da.concatenate([da.from_array(a, a.shape) for a in comp_l],
                                 axis=0).compute()

        # run prediction
        factors = pd.DataFrame(factors)
        dmat = sm.add_constant(factors)
        fit = _time_series_pred(resp, dmat)

        proxy_l.append(fit.resid)
        proxy = pd.DataFrame(proxy_l).T

        if out_dir is not None:
            proxy.to_csv(os.path.join(out_dir, "proxy.csv"), index=False)
            phi.to_csv(os.path.join(out_dir, "phi.csv"), index=False)
            factors.to_csv(os.path.join(out_dir, "factors.csv"), index=False)
            with open(os.path.join(out_dir, "fit.pkl"), "wb") as f:
                pickle.dump(fit, f)

    return proxy, phi, factors, fit
