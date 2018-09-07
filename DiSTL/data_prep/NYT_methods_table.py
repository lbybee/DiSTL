from global_methods_table import token_binner
from global_methods_general import tokenizer
from NYT_methods_general import doc_generator
from datetime import datetime
import os
import re


def gen_tables(doc, date, table_dir, ngrams, doc_id, tag_id_map_dict,
               term_id_map_dict):
    """generates the tables for the provided document

    Parameters
    ----------
    doc : json
        the raw source document
    date : str
        label for current file used for output document
    table_dir : str
        location where current tables are stored
    ngrams : int
        number of ngrams
    doc_id : int
        index of current document
    tag_id_map_dict : dict-like
        dictionary containing map from tag to tag id for each tag category
    term_id_map : dict-like
        dictionary containing map from term to term id for each ngram

    Returns
    -------
    None
    """

    doc_junk_chars = "[^ .a-zA-Z0-9]"
    tag_junk_chars = "[^ a-zA-Z0-9]"

    doc_metadata = []

    if "pub_date" in doc:
        doc_metadata.append(doc["pub_date"])
    else:
        doc_metadata.append("")

    if "web_url" in doc:
        doc_metadata.append(str(doc["web_url"]))
    else:
        doc_metadata.append("")

    for k in ["snippet", "lead_paragraph", "abstract",
              "print_page", "source", "document_type",
              "news_desk", "section_name", "word_count", "type_of_material",
              "_id"]:
        if k in doc:
            doc_metadata.append(re.sub(doc_junk_chars, "", str(doc[k])))
        else:
            doc_metadata.append("")

    if "headline" in doc:
        if "main" in doc["headline"]:
            if doc["headline"]["main"]:
                doc_metadata.append(re.sub(doc_junk_chars, "",
                                           doc["headline"]["main"]))
            else:
                doc_metadata.append("")
        else:
            doc_metadata.append("")
    else:
        doc_metadata.append("")

    if "byline" in doc:
        if doc["byline"]:
            if "original" in doc["byline"]:
                if doc["byline"]["original"]:
                    tmp = re.sub(doc_junk_chars, "",
                                 doc["byline"]["original"])
                    doc_metadata.append(tmp)
                else:
                    doc_metadata.append("")
            else:
                doc_metadata.append("")
        else:
            doc_metadata.append("")
    else:
        doc_metadata.append("")

    # handle tags
    if "keywords" in doc:
        if doc["keywords"]:
            keywords = doc["keywords"]
            keyword_dict = {}
            for k in keywords:
                name = k["name"].replace(",", "")
                value = re.sub(tag_junk_chars, "", k["value"])
                value = value.lower()
                value = value[:255]
                value = value.strip()
                if name not in keyword_dict:
                    keyword_dict[name] = []
                if value not in keyword_dict[name]:
                    keyword_dict[name].append(value)
                    tag_f = os.path.join(table_dir,
                                         "tag_link_%s_%s.csv" % (name,
                                                                 date))
                    try:
                        tag_id = tag_id_map_dict[name][value]
                        v = "%d,%d" % (doc_id, tag_id)
                        with open(tag_f, "a") as fd:
                            fd.write("%s\n" % v)
                    except:
                        continue

    # handle text
    doc_term_count_dict = {}
    for txt_cat in ["lead_paragraph", "abstract", "snippet"]:
        if txt_cat in doc:
            if doc[txt_cat]:
                tokens = tokenizer(doc[txt_cat])
                term_id_count_dict = token_binner(tokens, ngrams,
                                                  term_id_map_dict)
                doc_term_count_dict[txt_cat] = {}

                for term_ngram in term_id_count_dict:

                    doc_term_count_dict[txt_cat][term_ngram] = 0
                    count_f = os.path.join(table_dir,
                                           "count_%s_%s_%s.csv" % (txt_cat,
                                                                   term_ngram,
                                                                   date))

                    for term_id in term_id_count_dict[term_ngram]:
                        term_count = term_id_count_dict[term_ngram][term_id]
                        doc_term_count_dict[txt_cat][term_ngram] += term_count
                        v = "%d,%d,%d" % (doc_id, term_id, term_count)
                        with open(count_f, "a") as fd:
                            fd.write("%s\n" % v)

    # add computed term counts to doc metadata
    for txt_cat in ["lead_paragraph", "abstract", "snippet"]:
        if txt_cat in doc_term_count_dict:
            doc_metadata.append(str(doc_term_count_dict[txt_cat]["1gram"]))
        else:
            doc_metadata.append("0")
    doc_metadata = [r if r != "None" else "" for r in doc_metadata]
    doc_metadata = ["%s" % doc_id] + doc_metadata
    with open(os.path.join(table_dir, "doc_%s.csv" % date), "a") as fd:
        fd.write(",".join(doc_metadata) + "\n")

    doc_id += 1

    return doc_id


def table_file_processor(f, raw_dir, table_dir, ngrams, tag_id_map_dict,
                         term_id_map_dict, log_file):
    """handles the file processing to produce tables

    Parameters
    ----------
    f : str
        file name
    raw_dir : str
        location where raw files are stored
    table_dir : str
        location where current tables are stored
    ngrams : int
        number of ngrams
    tag_id_map_dict : dict-like
        dictionary containing map from tag to tag id for each tag category
    term_id_map : dict-like
        dictionary containing map from term to term id for each ngram
    log_file : str
        location of log file

    Returns
    -------
    None
    """

    # initialize params
    t0 = datetime.now()
    date = f.replace(".json", "")

    # seed current tables with column names

    # docs
    columns = ["doc_id", "pub_date", "web_url", "snippet", "lead_paragraph",
               "abstract", "print_page", "source", "document_type",
               "news_desk", "section_name", "word_count", "type_of_material",
               "nyt_id", "headline", "byline", "lead_paragraph_term_count",
               "abstract_term_count", "snippet_term_count"]
    with open(os.path.join(table_dir, "doc_%s.csv" % date), "w") as fd:
        fd.write("%s\n" % ",".join(columns))

    # terms
    for txt_lab in ["lead_paragraph", "abstract", "snippet"]:
        for ngram in term_id_map_dict:
            with open(os.path.join(table_dir,
                                   "count_%s_%s_%s.csv" % (txt_lab, ngram,
                                                           date)),
                      "w") as fd:
                fd.write("doc_id,term_id,count\n")

    # tags
    for tag_cat in tag_id_map_dict:
        with open(os.path.join(table_dir,
                               "tag_link_%s_%s.csv" % (tag_cat, date)),
                  "w") as fd:
            fd.write("doc_id,tag_id\n")

    # get generator and prep args
    generator = doc_generator(os.path.join(raw_dir, f))

    doc_id = 0
    for doc in generator:
        doc_id = gen_tables(doc, date, table_dir, ngrams, doc_id,
                            tag_id_map_dict, term_id_map_dict)

    t1 = datetime.now()
    with open(log_file, "a") as fd:
        fd.write("%s,%s,%s\n" % (f, str(t1), str(t1 - t0)))
