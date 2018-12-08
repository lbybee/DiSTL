def topic_model_wrapper(in_data_dir, out_data_dir, method_kwds=None):
    """wrapper for estimating general topic models which interacts with
    data and preps model state

    Parameters
    ----------
    in_data_dir : str
        location of DTM files
    out_data_dir : str
        location where results will be written
    method_kwds : dict-like or None
        key-words to pass to specific method

    Returns
    -------
    None

    Writes
    ------
    topic model results
    """
