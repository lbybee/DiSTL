import os

# directory names
dir_path = os.path.dirname(os.path.realpath(__file__))
project_dir = "/home/lbybee/Dropbox/BK_LB_Projects/DJN_comp_cs_returns"
project_dir = os.path.join(project_dir, "intermediate")

# db info
db = "production"
processes = 24
ngrams = 2
source_dir = "/home/lbybee/Dropbox/BK_LB_Projects/Text_Data_API"
source_dir = os.path.join(source_dir, "intermediate", "DJ_newswire",
                          "unzipped_files_WSJ")
dates = [f.replace("-", "_").replace(".nml", "")
         for f in os.listdir(source_dir)]

# txt labels
txt_labels = ["body", "headline"]

# tags
fname = os.path.join(dir_path, "drop_subject")
subject_l = [r for r in open(fname, "r").read().split("\n") if r != ""]
fname = os.path.join(dir_path, "drop_product")
product_l = [r for r in open(fname, "r").read().split("\n") if r != ""]
fname = os.path.join(dir_path, "drop_stat")
stat_l = [r for r in open(fname, "r").read().split("\n") if r != ""]
tag_drop_dict = {"djn_subject": subject_l,
                 "djn_product": product_l,
                 "djn_stat": stat_l}

# headline and author info
fname = os.path.join(dir_path, "drop_headlines")
headline_l = [r for r in open(fname, "r").read().split("\n") if r != ""]
fname = os.path.join(dir_path, "drop_authors")
author_l = [r for r in open(fname, "r").read().split("\n") if r != ""]

# stop words
ngram_stop_words_dict = {}
fname = os.path.join(dir_path, "stop_words")
terms = [t for t in open(fname, "r").read().split("\n") if t != ""]
ngram_stop_words_dict["1gram"] = terms
ngram_stop_words_dict["2gram"] = []
for w in terms:
    ngram_stop_words_dict["2gram"].extend(["%" + (" %s" % w), ("%s " % w) +
                                           "%"])

# regex stop words
regex_stop_words = []
fname = os.path.join(dir_path, "regex_stop_words")
regex_stop_words += [t for t in open(fname, "r").read().split("\n") if t != ""]
ngram_regex_stop_words_dict = {"%dgram" % n: regex_stop_words for
                               n in range(1, ngrams + 1)}

# thresholds
ngram_term_threshold_dict = {"%dgram" % n: 1000
                             for n in range(1, ngrams + 1)}
doc_threshold = 100

# company id info
f = os.path.join(dir_path, "tickers.csv")
company_id_l = open(f, "r").read().split("\n")[:-1]
