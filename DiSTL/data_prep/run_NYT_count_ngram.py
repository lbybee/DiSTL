from NYT_methods_count import ngram_file_processor
from global_methods_count import ngram_wrapper
import os


# ---------- #
# Parameters #
# ---------- #

# misc params
n = 2
processes = 48
doc_lthresh = 10
txt_labels = ["lead_paragraph", "abstract", "snippet"]

# directories
in_data_dir = os.path.join("/home/lbybee/Dropbox/BK_LB_Projects",
                           "Text_Data_API/raw/NYT")
tmp_dir = "/home/lbybee/scratch_local/NYT/raw_counts"
out_count_dir = "/home/lbybee/scratch_local/NYT/counts"

# files
stop_word_files = ["config/NYT/stop_words"]
regex_stop_word_files = ["config/NYT/regex_stop_words"]
log_file = "/home/lbybee/scratch_local/NYT/%dgram.log" % n
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
