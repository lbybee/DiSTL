from DJ_methods_count import ngram_file_processor
from global_methods_count import ngram_wrapper
import os


# ---------- #
# Parameters #
# ---------- #

# misc params
n = 2
processes = 24
doc_lthresh = 10
txt_labels = ["body", "headline"]

# directories
in_data_dir = os.path.join("/home/lbybee/Dropbox/BK_LB_Projects",
                           "Text_Data_API/intermediate/DJ_newswire",
                           "unzipped_files")
tmp_dir = "/home/lbybee/scratch_local/DJN/raw_counts"
out_count_dir = "/home/lbybee/scratch_local/DJN/counts"

# files
stop_word_files = ["config/DJN/stop_words",
                   "config/DJN/stop_words_DJN"]
regex_stop_word_files = ["config/DJN/regex_stop_words",
                         "config/DJN/regex_stop_words_DJN"]
log_file = "/home/lbybee/scratch_local/DJN/%dgram.log" % n
raw_files = os.listdir(in_data_dir)

# term aggregates
term_columns = ["term_label", "doc_count", "term_count"]
term_groupby_col = "term_label"
term_index_name = "term_id"


# -------- #
# Run Code #
# -------- #

if __name__ == "__main__":

    ngram_wrapper(n, processes, doc_lthresh, in_data_dir, tmp_dir,
                  out_count_dir, stop_word_files, regex_stop_word_files,
                  log_file, raw_files, txt_labels,
                  term_columns, term_groupby_col, term_index_name,
                  ngram_file_processor)
