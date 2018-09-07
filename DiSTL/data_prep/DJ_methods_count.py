from global_methods_count import unigram_counter, ngram_counter
from global_methods_general import tokenizer
from DJ_methods_general import doc_generator
from datetime import datetime
import os


def gen_unigram_tag_counts(doc, term_dict, tag_dict):
    """generates all the unique unigrams and tags for the corresponding
    document and inserts them into the appropriate count dictionary

    Parameters
    ----------
    doc : XML document
        raw source document
    term_dict : dictionary
        dictionary containing map from term to count for headlines and body
    tag_dict : dictionary
        dict where every element is itself a dictionary mapping the unique
        tags for a given tag category to their total counts

    Returns
    -------
    updated term_dict and tag_dict
    """

    # extract txt from XML
    djnml = doc.find("djnml")
    djnml_attrib = djnml.attrib
    head = djnml.find("head")
    newswires = head.find("docdata").find("djn").find("djn-newswires")
    newswires_attrib = newswires.attrib
    mdata = newswires.find("djn-mdata")
    mdata_attrib = mdata.attrib
    body = djnml.find("body")
    headline = body.find("headline").text
    headline = " ".join(headline.split())
    body_string = ""
    for t in body.find("text").getchildren():
        body_string += " %s " % t.text

    # process txt and insert terms into dict
    term_dict["body"] = unigram_counter(body_string, term_dict["body"])
    term_dict["headline"] = unigram_counter(headline, term_dict["headline"])

    # handle tags
    coding = mdata.find("djn-coding")
    codes = coding.getchildren()

    for c in codes:

        tag_cat = c.tag.replace("-", "_")

        if tag_cat not in tag_dict:
            tmp_dict = {}
        else:
            tmp_dict = tag_dict[tag_cat]

        # clean unique tag labels (for given tag_cat)
        children = set()
        for t in c.getchildren():
            t = t.text
            if t:
                t = t.replace(",", "").replace("\\", "")
                t = " ".join(t.split())
                children.add(t)

        for t in children:
            try:
                tmp_dict[t] += 1
            except:
                tmp_dict[t] = 1

        tag_dict[tag_cat] = tmp_dict

    return term_dict, tag_dict


def gen_ngram_counts(doc, unigram_map, n, term_dict):
    """generates all the unique n-grams for the corresponding document
    and inserts into the count dict

    Parameters
    ----------
    doc : XML document
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

    # extract txt from XML
    djnml = doc.find("djnml")
    djnml_attrib = djnml.attrib
    head = djnml.find("head")
    newswires = head.find("docdata").find("djn").find("djn-newswires")
    newswires_attrib = newswires.attrib
    mdata = newswires.find("djn-mdata")
    mdata_attrib = mdata.attrib
    body = djnml.find("body")
    headline = body.find("headline").text
    headline = " ".join(headline.split())
    body_string = ""
    for t in body.find("text").getchildren():
        body_string += " %s " % t.text

    # process txt and insert terms into dict
    term_dict["body"] = ngram_counter(body_string, term_dict["body"],
                                      unigram_map, n)
    term_dict["headline"] = ngram_counter(headline, term_dict["headline"],
                                          unigram_map, n)

    return term_dict


def unigram_tag_file_processor(f, in_data_dir, out_data_dir, log_file):
    """extracts all the unigram counts and tag counts from the corresponding
    file and writes to several csvs

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

    # get generator and prep args
    generator = doc_generator(os.path.join(in_data_dir, f))
    date_label = f.replace("-", "_").replace(".nml", "")

    term_dict = {"headline": {}, "body": {}}
    tag_dict = {}
    for doc in generator:
        term_dict, tag_dict = gen_unigram_tag_counts(doc, term_dict,
                                                     tag_dict)

    # write counts
    for txt_cat in term_dict:
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

    # get generator and prep args
    generator = doc_generator(os.path.join(in_data_dir, f))
    date_label = f.replace("-", "_").replace(".nml", "")

    term_dict = {"headline": {}, "body": {}}
    for doc in generator:
        term_dict = gen_ngram_counts(doc, unigram_map, n, term_dict)

    for txt_cat in term_dict:
        with open(os.path.join(out_data_dir, "%s_%s_%dgram.csv" % (date_label,
                                                                   txt_cat,
                                                                   n)),
                  "w") as fd:
            for c in term_dict[txt_cat]:
                fd.write("%s,%d,%d\n" % (c,
                                         term_dict[txt_cat][c]["doc_count"],
                                         term_dict[txt_cat][c]["term_count"]))

    t1 = datetime.now()
    with open(log_file, "a") as fd:
        fd.write("%s,%s,%s\n" % (f, str(t1), str(t1 - t0)))
