from global_methods_count import unigram_counter, ngram_counter
from NYT_methods_general import doc_generator
from datetime import datetime
import os
import re


def gen_unigram_tag_counts(doc, term_dict, tag_dict):
    """generates doc counts and term counts for all unigrams and generates
    doc counts for all tags

    Parameters
    ----------
    doc : json document
        raw source document
    term_dict : dictionary
        dictionary containing a dict for each type of text, mapping terms to
        doc and term counts
    tag_dict : dictionary
        dictionary containing a dict for each tag mapping tags to counts

    Returns
    -------
    updated term_dict, and tag_dict
    """

    junk_chars = "[^ a-zA-Z]"

    for txt_cat in ["lead_paragraph", "abstract", "snippet"]:
        if txt_cat in doc:
            if doc[txt_cat]:
                tmp_dict = term_dict[txt_cat]
                tmp_dict = unigram_counter(doc[txt_cat], tmp_dict)
                term_dict[txt_cat] = tmp_dict

    if doc["keywords"]:
        keywords = doc["keywords"]
        keyword_dict = {}
        for k in keywords:
            name = k["name"].replace(",", "")
            value = re.sub(junk_chars, "", k["value"])
            value = value.lower()
            value = value[:255]
            value = value.strip()
            if name not in keyword_dict:
                keyword_dict[name] = []
            if value not in keyword_dict[name]:
                keyword_dict[name].append(value)
                if name not in tag_dict:
                    tmp_dict = {}
                else:
                    tmp_dict = tag_dict[name]

                try:
                    tmp_dict[value] += 1
                except:
                    tmp_dict[value] = 1

                tag_dict[name] = tmp_dict

    return term_dict, tag_dict


def gen_ngram_counts(doc, unigram_map, n, term_dict):
    """generates all the unique n-grams for the corresponding document
    and inserts into the count dict

    Parameters
    ----------
    doc : json document
        raw source document
    unigram_map : dict-like
        data-frame or dictionary mapping terms to stemmed/lemmatized version
        only includes terms which we keep
    n : int
        n for n-gram
    term_dict : dictionary
        dictionary mapping terms to counts

    Returns
    -------
    updated term_dict
    """

    for txt_cat in ["lead_paragraph", "abstract", "snippet"]:
        if txt_cat in doc:
            if doc[txt_cat]:
                term_dict[txt_cat] = ngram_counter(doc[txt_cat],
                                                   term_dict[txt_cat],
                                                   unigram_map, n)

    return term_dict


def unigram_tag_file_processor(f, in_data_dir, out_data_dir, log_file):
    """extracts all the unigram counts from the corresponding file
    and writes to a csv

    Parameters
    ----------
    f : str
        file name
    in_data_dir : str
        location where input file is stored
    out_data_dir : str
        location where output file will be stored
    log_file : str
        location of log file

    Returns
    -------
    None
    """

    t0 = datetime.now()
    txt_categories = ["lead_paragraph", "abstract", "snippet"]
    date_label = f.replace(".json", "")

    # get generator and prep args
    generator = doc_generator(os.path.join(in_data_dir, f))

    term_dict = {txt_cat: {} for txt_cat in txt_categories}
    tag_dict = {}
    for doc in generator:
        term_dict, tag_dict = gen_unigram_tag_counts(doc, term_dict,
                                                     tag_dict)

    # write counts
    for txt_cat in txt_categories:
        with open(os.path.join(out_data_dir, "%s_%s_1gram.csv" % (date_label,
                                                                  txt_cat)),
                  "w") as fd:
            for c in term_dict[txt_cat]:
                fd.write("%s,%d,%d\n" % (c,
                                         term_dict[txt_cat][c]["doc_count"],
                                         term_dict[txt_cat][c]["term_count"]))

    # write tags
    for tag_cat in tag_dict:
        with open(os.path.join(out_data_dir, "%s_%s.csv" % (date_label,
                                                            tag_cat)),
                  "w") as fd:
            for t in tag_dict[tag_cat]:
                fd.write("%s,%d\n" % (t, tag_dict[tag_cat][t]))

    t1 = datetime.now()
    with open(log_file, "a") as fd:
        fd.write("%s,%s,%s\n" % (f, str(t1), str(t1 - t0)))


def ngram_file_processor(f, in_data_dir, out_data_dir, unigram_map, n,
                         log_file):
    """extracts all the n-gram counts from the corresponding file
    and writes to a csv

    Parameters
    ----------
    f : str
        file name
    in_data_dir : str
        location where input file is stored
    out_data_dir : str
        location where output file will be stored
    unigram_map : dict-like
        data-frame or dictionary mapping terms to stemmed/lemmatized version
        only includes terms which we keep
    n : int
        n for n-gram
    log_file : str
        location of log file

    Returns
    -------
    None
    """

    t0 = datetime.now()
    txt_categories = ["lead_paragraph", "abstract", "snippet"]

    # get generator and prep args
    generator = doc_generator(os.path.join(in_data_dir, f))

    term_dict = {txt_cat: {} for txt_cat in txt_categories}
    for doc in generator:
        term_dict = gen_ngram_counts(doc, unigram_map, n, term_dict)

    # write counts
    for txt_cat in txt_categories:
        with open(os.path.join(out_data_dir,
                               f.replace(".json", "_%s_%dgram.csv" % (txt_cat,
                                                                      n))),
                  "w") as fd:
            for c in term_dict[txt_cat]:
                fd.write("%s,%d,%d\n" % (c,
                                         term_dict[txt_cat][c]["doc_count"],
                                         term_dict[txt_cat][c]["term_count"]))

    t1 = datetime.now()
    with open(log_file, "a") as fd:
        fd.write("%s,%s,%s\n" % (f, str(t1), str(t1 - t0)))
