from .global_methods_general import tokenizer, vocab_cleaner, \
                                    default_lemmatizer
from datetime import datetime
import .NYT_methods_count as NYTc
import .DJ_methods_count as DJc
import dask.dataframe as dd
import multiprocessing
import pandas as pd
import os


def unigram_counter(text_string, term_dict):
    """extracts the unique unigrams from a string and inserts the additional
    doc_counts and term_counts into the provided term_dict

    Parameters
    ----------
    text_string : string
        raw txt string for document
    term_dict : dict-like
        dictionary mapping terms to doc_count, term_count dicts

    Returns
    -------
    updated term_dict
    """

    text_list = tokenizer(text_string)
    text_set = set(text_list)

    for t in text_set:
        try:
            term_dict[t]["doc_count"] += 1
        except:
            term_dict[t] = {"doc_count": 1,
                             "term_count": 0}
    for t in text_list:
        term_dict[t]["term_count"] += 1

    return term_dict


def ngram_counter(text_string, term_dict, unigram_map, n):
    """extracts the doc_counts and term_counts from the specified document
    and inserts into the provided term_dict

    Parameters
    ----------
    text_string : string
        raw txt string for document
    term_dict : dict-like
        dictionary mapping terms to doc_count, term_count dicts
    unigram_map : dict-like
        mapping from raw term to cleaned unigram. This is used to clean the
        terms used to generate the n-grams.  Note that in caes where a
        "cleaned" unigram doesn't exist (e.g. was dropped) we'll drop it
        before creating n-gram
    n : int
        n in n-gram

    Notes
    -----
    There are two reasons process_unigrams and process_ngrams can't be rolled
    into one method

    1. the processing for process_unigrams is slightly cheaper so it doesn't
       make sense to use process_ngrams for the n=1 case
    2. the n-grams we generate are conditional on the cleaning methods used
       to generate unigram_map, therefore we wouldn't have a unigram_map for
       n=1

    Returns
    -------
    updated term_dict
    """

    text_list = tokenizer(text_string)
    n_text_list = []
    for t in text_list:
        try:
            t = unigram_map[t]
            n_text_list.append(t)
        except:
            continue

    text_list = [" ".join(tp) for tp in zip(*[n_text_list[i:] for i in
                                              range(n)])]
    text_set = set(text_list)

    for t in text_set:
        try:
            term_dict[t]["doc_count"] += 1
        except:
            term_dict[t] = {"doc_count": 1,
                             "term_count": 0}
    for t in text_list:
        term_dict[t]["term_count"] += 1

    return term_dict


def aggregate_counts(tmp_dir, out_count_dir, f_label, columns,
                     groupby_col, index_name, log_file):
    """aggregates the count files produced for each partition

    Parameters
    ----------
    tmp_dir : str
        location where partitioned count files are located
    out_count_dir : str
        location where aggregate while will be stored
    f_label : str
        label for aggregate, note that the partitioned count files should be
        of form *_<f_label>.csv and the output file will be <f_label>.csv
    columns : str
        list of column names
    groupby_col : str
        column label on which to group by for aggregate
    index_name : str
        name for index
    log_file : str
        location where log results should be stored

    Returns
    -------
    None
    """

    t0 = datetime.now()
    counts = dd.read_csv(os.path.join(tmp_dir, "*_%s.csv" % f_label),
                         names=columns, dtype={groupby_col: str})
    counts = counts.groupby(groupby_col).sum()
    counts = counts.compute()
    counts = counts.reset_index()
    counts.index.name = index_name
    counts.to_csv(os.path.join(out_count_dir, "%s.csv" % f_label))
    t1 = datetime.now()
    with open(log_file, "a") as fd:
        fd.write("%s,%s,%s\n" % (f_label, str(t1), str(t1 - t0)))



def gen_unigram_map(count_file_l, raw_term_label, stop_word_files=None,
                    regex_stop_word_files=None, doc_lthresh=None):
    """generates a map from the raw to clean unigrams based on the provided
    input for the corresponding count file

    Parameters
    ----------
    count_file_l : list
        list of count file locations to use for unigram map
    raw_term_label : str
        label in count_file of the column which will become "raw_term"
    stop_word_files : list or None
        list of files containing stop words
    regex_stop_word_files : list or None
        list of files containing regex stop words
    doc_lthresh : int or None
        threshold below which to drop terms

    Returns
    -------
    dictionary mapping raw terms to clean terms
    """

    if stop_word_files:
        stop_words = []
        for swf in stop_word_files:
            stop_words.extend([t for t in open(swf, "r").read().split("\n")
                               if t != ""])
    else:
        stop_words = None

    if regex_stop_word_files:
        regex_stop_words = []
        for rswf in regex_stop_word_files:
            regex_stop_words.extend([t for t in
                                     open(rswf, "r").read().split("\n")
                                     if t != ""])
    else:
        regex_stop_words = None

    full_unigram_map = pd.DataFrame([], columns=["raw_term", "count"])

    for count_file in count_file_l:
        unigram_map = pd.read_csv(count_file)
        unigram_map["count"] = unigram_map["doc_count"]
        unigram_map["raw_term"] = unigram_map[raw_term_label]
        unigram_map = unigram_map.dropna()
        unigram_map.index = unigram_map["raw_term"]
        unigram_map = unigram_map[["count"]]
        full_unigram_map = full_unigram_map.add(unigram_map, fill_value=0)

    full_unigram_map["raw_term"] = full_unigram_map.index
    full_unigram_map = vocab_cleaner(full_unigram_map, stop_words=stop_words,
                                     regex_stop_words=regex_stop_words,
                                     doc_lthresh=doc_lthresh,
                                     stemmer=default_lemmatizer)
    full_unigram_map = full_unigram_map[["raw_term", "term"]]
    full_unigram_map = full_unigram_map["term"].to_dict()

    return full_unigram_map


def unigram_tag_wrapper(processes, in_data_dir, tmp_dir, out_count_dir,
                        log_file, raw_files, txt_labels, term_columns,
                        term_groupby_col, term_index_name, tag_columns,
                        tag_groupby_col, tag_index_name,
                        file_processor_label):
    """generates the aggregate count file for the unigram terms as well as
    tags from raw data

    Parameters
    ----------
    processes : int
        number of processes for multiprocessing pool
    in_data_dir : str
        location where raw files are stored
    tmp_dir : str
        location where temporary files are stored
    out_count_dir : str
        location where final count files will be stored
    log_file : str
        location of log file
    raw_files : list
        list of file names for raw files (doesn't include directory)
    txt_labels : list
        list of txt categories/labels (e.g. body, headline)
    <term/tag>_columns : str
        list of column names
    <term/tag>_groupby_col : str
        column label on which to group by for aggregate
    <term/tag>_index_name : str
        name for index
    data_type : str
        label for file processor used for this run

    Returns
    -------
    None
    """

    if data_type == "DJ":
        unigram_tag_file_processor = DJc.unigram_tag_file_processor
    elif data_type == "NYT":
        unigram_tag_file_processor = NYTc.unigram_tag_file_processor
    else:
        raise ValueError("Unsupported data_type %s" % data_type)

    term_f_label_l = ["%s_1gram" % l for l in txt_labels]

    # generate counts from raw data
    t0 = datetime.now()
    pool = multiprocessing.Pool(processes)

    pool.starmap(unigram_tag_file_processor, [(f, in_data_dir, tmp_dir,
                                               log_file)
                                              for f in raw_files])

    # aggregate terms
    for term_f_label in term_f_label_l:
        aggregate_counts(tmp_dir, out_count_dir, term_f_label,
                         term_columns, term_groupby_col, term_index_name,
                         log_file)

    # aggregate tags
    # TODO get tags more cleanly can cause bugs
    tmp_files = os.listdir(tmp_dir)
    tags = set(["_".join(f.split(".")[0].split("_")[2:])
                for f in tmp_files])
    tags = [t for t in tags if t not in term_f_label_l]
    for t in tags:
        aggregate_counts(tmp_dir, out_count_dir, t, tag_columns,
                         tag_groupby_col, tag_index_name, log_file)

    # remove intermediate files
    out_files = os.listdir(tmp_dir)
    for f in out_files:
        os.remove(os.path.join(tmp_dir, f))


def ngram_wrapper(n, processes, doc_lthresh, in_data_dir, tmp_dir,
                  out_count_dir, stop_word_files, regex_stop_word_files,
                  log_file, raw_files, txt_labels,
                  term_columns, term_groupby_col, term_index_name,
                  file_processor_label):
    """generates the aggregate count file for the ngram terms fro raw data

    Parameters
    ----------
    n : int
        n in n-gram
    processes : int
        number of processes for multiprocessing pool
    doc_lthresh : int
        lower threshold for dropping terms for unigram_map
    in_data_dir : str
        location where raw files are stored
    tmp_dir : str
        location where temporary files are stored
    out_count_dir : str
        location where final count files will be stored
    stop_word_files : list
        list of file containing stop words for unigram_map
    regex_stop_word_files : list
        list of files containing regex stop words for unigram_map
    log_file : str
        location of log file
    raw_files : list
        list of file names for raw files (doesn't include directory)
    txt_labels : list
        list of labels for txt categories (e.g. body, headline)
    term_columns : str
        list of column names
    term_groupby_col : str
        column label on which to group by for aggregate
    term_index_name : str
        name for index
    data_type : str
        label for file processor used for this run

    Returns
    -------
    None
    """

    if data_type == "DJ":
        ngram_file_processor = DJc.ngram_file_processor
    elif data_type == "NYT":
        ngram_file_processor = NYTc.ngram_file_processor
    else:
        raise ValueError("Unsupported data_type %s" % data_type)

    unigram_map_files = [os.path.join(out_count_dir, "%s_1gram.csv" % l)
                         for l in txt_labels]
    term_f_label_l = ["%s_%dgram" % (l, n) for l in txt_labels]

    # prep unigram_map
    unigram_map = gen_unigram_map(unigram_map_files, "term_label",
                                  stop_word_files,
                                  regex_stop_word_files, doc_lthresh)

    # generate counts from raw data
    t0 = datetime.now()
    pool = multiprocessing.Pool(processes)

    pool.starmap(ngram_file_processor, [(f, in_data_dir, tmp_dir,
                                         unigram_map, n, log_file)
                                        for f in raw_files])

    # aggregate counts
    for term_f_label in term_f_label_l:
        aggregate_counts(tmp_dir, out_count_dir, term_f_label,
                         term_columns, term_groupby_col, term_index_name,
                         log_file)

    # remove intermediate files
    out_files = os.listdir(tmp_dir)
    for f in out_files:
        os.remove(os.path.join(tmp_dir, f))
