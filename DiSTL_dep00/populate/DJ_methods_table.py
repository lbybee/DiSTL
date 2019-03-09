from .global_methods_table import token_binner
from .global_methods_general import tokenizer
from .DJ_methods_general import doc_generator
from datetime import datetime
import os
import re


def gen_tables(doc, current_date, offset_date, table_dir, ngrams,
               current_doc_id, offset_doc_id, tag_id_map_dict,
               term_id_map_dict):
    """generates the tables for the provided document

    Parameters
    ----------
    doc : XML document
        the raw source document
    <current|offset>_date : str
        label for the <current|offset> file for cases where accession_number
        <does|doesn't> match display_date (meaning current_date
        <does|doesn't> match display_date
    table_dir : str
        location where current tables are stored
    ngrams : int
        number of ngrams
    <current|offset>_doc_id : int
        index for current document if the documents display_date
        <does|doesn't> match the accession_number.  This can be an issue
        because DJN produces the source files based on accession_number
        not display_date
    tag_id_map_dict : dict-like
        dictionary containing map from tag to tag id for each tag category
    term_id_map : dict-like
        dictionary containing map from term to term id for each ngram

    Returns
    -------
    updated current_doc_id and offset_doc_id
    """

    # extract xml values
    junk_chars = "[^ .a-zA-Z0-9]"
    djnml = doc.find("djnml")
    djnml_attrib = djnml.attrib
    head = djnml.find("head")
    newswires = head.find("docdata").find("djn").find("djn-newswires")
    newswires_attrib = newswires.attrib
    mdata = newswires.find("djn-mdata")
    mdata_attrib = mdata.attrib
    body = djnml.find("body")
    text = ""
    for t in body.find("text").getchildren():
        text += " %s " % t.text
    text = re.sub(junk_chars, "", text)
    headline = body.find("headline").text
    headline = re.sub(junk_chars, "", headline)
    headline = " ".join(headline.split())
    txt_strings = {"body": text, "headline": headline}

    # handle docs
    # note that we wait to write this until we've got the
    # term counts
    doc_keys_djnml = ("publisher", "product", "seq", "docdate")
    doc_keys_newswires = ("news-source", "origin", "service-id")
    doc_keys_mdata = ("brand", "temp-perm", "retention", "hot",
                      "original-source", "accession-number",
                      "page-citation", "display-date")
    doc_keys_txt = ("headline", "body-txt")
    doc_metadata = []
    for k in doc_keys_djnml:
        doc_metadata.append(djnml_attrib[k].replace(",", "").replace('"', ""))
    for k in doc_keys_newswires:
        doc_metadata.append(newswires_attrib[k].replace(",", "").replace('"', ""))
    for k in doc_keys_mdata:
        doc_metadata.append(mdata_attrib[k].replace(",", "").replace('"', ""))
    doc_metadata.append(headline)
    doc_metadata.append(text)

    # handle edge cases where display_date is NOT within current month
    display_date = mdata_attrib["display-date"][:6]
    if display_date == current_date.replace("_", ""):
        date = current_date
        doc_id = current_doc_id
        current_doc_id += 1
        file_label = "current"
    else:
        date = offset_date
        doc_id = offset_doc_id
        offset_doc_id += 1
        file_label = "temp"
    doc_metadata = [str(doc_id)] + doc_metadata

    # handle tags
    coding = mdata.find("djn-coding")
    codes = coding.getchildren()

    for c in codes:
        tag_cat = c.tag.replace("-", "_")
        tag_f = os.path.join(table_dir,
                             "tag_link_%s_%s_%s.csv" % (tag_cat,
                                                        date,
                                                        file_label))

        children = set()
        for t in c.getchildren():
            t = t.text
            if t:
                t = t.replace(",", "").replace("\\", "")
                t = " ".join(t.split())
                children.add(t)

        for t in children:
            try:
                tag_id = tag_id_map_dict[tag_cat][t]
                v = "%d,%d" % (doc_id, tag_id)
                with open(tag_f, "a") as fd:
                    fd.write("%s\n" % v)
            except:
                continue

    # handle text
    doc_term_count_dict = {}
    for txt_cat in ["headline", "body"]:
        tokens = tokenizer(txt_strings[txt_cat])
        term_id_count_dict = token_binner(tokens, ngrams, term_id_map_dict)
        doc_term_count_dict[txt_cat] = {}

        for term_ngram in term_id_count_dict:

            doc_term_count_dict[txt_cat][term_ngram] = 0
            count_f = os.path.join(table_dir,
                                   "count_%s_%s_%s_%s.csv" % (txt_cat,
                                                              term_ngram,
                                                              date,
                                                              file_label))

            for term_id in term_id_count_dict[term_ngram]:
                term_count = term_id_count_dict[term_ngram][term_id]
                doc_term_count_dict[txt_cat][term_ngram] += term_count
                v = "%d,%d,%d" % (doc_id, term_id, term_count)
                with open(count_f, "a") as fd:
                    fd.write("%s\n" % v)

    # add doc count to end of doc table for each txt cat
    # note that we only add the unigrams for the term count because
    # the term count for (n)gram is 1gram + 1 - n
    for txt_cat in ["headline", "body"]:
        doc_metadata.append(str(doc_term_count_dict[txt_cat]["1gram"]))
    with open(os.path.join(table_dir, "doc_%s_%s.csv" % (date,
                                                         file_label)),
              "a") as fd:
        fd.write(",".join(doc_metadata) + "\n")

    return current_doc_id, offset_doc_id


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

    # initialize params for gen_tables
    t0 = datetime.now()
    date = f.replace(".nml", "").replace("-", "_")
    current_doc_id = 0
    offset_doc_id = 0
    current_date = date
    year, month = date.split("_")
    if int(month) == 12:
        offset_date = "%s_01" % str(int(year) + 1)
    else:
        offset_date = "%s_%s" % (year, str(int(month) + 1).zfill(2))

    # seed current tables with column names

    # docs
    columns = ["doc_id", "publisher", "product", "seq", "docdate",
               "news_source", "origin", "service_id", "brand",
               "temp_perm", "retention", "hot", "original_source",
               "accession_number", "page_citation", "display_date",
               "headline", "body_txt", "headline_term_count",
               "body_term_count"]
    with open(os.path.join(table_dir, "doc_%s_current.csv" % date),
              "w") as fd:
        fd.write("%s\n" % ",".join(columns))

    # terms
    for txt_lab in ["headline", "body"]:
        for ngram in term_id_map_dict:
            with open(os.path.join(table_dir,
                                   "count_%s_%s_%s_current.csv" % (txt_lab,
                                                                   ngram,
                                                                   date)),
                      "w") as fd:
                fd.write("doc_id,term_id,count\n")

    # tags
    for tag_cat in tag_id_map_dict:
        with open(os.path.join(table_dir,
                               "tag_link_%s_%s_current.csv" % (tag_cat,
                                                               date)),
                  "w") as fd:
            fd.write("doc_id,tag_id\n")

    # get generator and prep args
    generator = doc_generator(os.path.join(raw_dir, f))

    for doc in generator:
        current_doc_id, offset_doc_id = gen_tables(doc, current_date,
                                                   offset_date, table_dir,
                                                   ngrams, current_doc_id,
                                                   offset_doc_id,
                                                   tag_id_map_dict,
                                                   term_id_map_dict)

    t1 = datetime.now()
    with open(log_file, "a") as fd:
        fd.write("%s,%s,%s\n" % (f, str(t1), str(t1 - t0)))


def table_post_cleaner(f, table_dir, log_file):
    """takes a table which may be broken into a current and temp component
    and combines into one (removing the temporary component).  This is used
    to handle the documents where the accession number and months
    don't align

    Parameters
    ----------
    f : str
        file name
    table_dir : str
        location where current tables are stored
    log_file : str
        location of log file

    Returns
    -------
    None
    """

    # initialize params
    t0 = datetime.now()
    date = f.replace(".nml", "").replace("-", "_")

    current_files = []
    temporary_files = []
    for tf in os.listdir(table_dir):
        if date in tf:
            if "current" in tf:
                current_files.append(tf)
            elif "temp" in tf:
                temporary_files.append(tf)

    # add temp contents to main file (if they exist)
    if len(temporary_files) > 0:

        # get offset if needed
        doc_file = os.path.join(table_dir, "doc_%s_current.csv" % date)
        last_line = os.popen("tail -n 1 %s" % doc_file).read()
        doc_id = int(last_line.split(",")[0])
        for tf in temporary_files:
            cf = tf.replace("temp", "current")
            with open(os.path.join(table_dir, tf), "r") as fd:
                content = fd.read().split("\n")[:-1]
            for line in content:
                line_split = line.split(",")
                line_split[0] = str(int(line_split[0]) + doc_id + 1)
                with open(os.path.join(table_dir, cf), "a") as fd:
                    fd.write("%s\n" % ",".join(line_split))

    # rename main file and destroy temp files
    for tf in temporary_files:
        os.remove(os.path.join(table_dir, tf))
    for cf in current_files:
        tcf = cf.replace("_current", "")
        os.rename(os.path.join(table_dir, cf), os.path.join(table_dir, tcf))

    t1 = datetime.now()
    with open(log_file, "a") as fd:
        fd.write("%s,%s,%s\n" % (f, str(t1), str(t1 - t0)))
