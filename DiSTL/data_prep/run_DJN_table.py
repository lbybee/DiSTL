from DJ_methods_table import table_file_processor, table_post_cleaner
from global_methods_table import table_wrapper
import os

# ---------- #
# Parameters #
# ---------- #

# misc params
n = 2
processes = 16
txt_labels = ["body", "headline"]
doc_lthresh_1gram = 10
doc_lthresh_2gram = 50

# directories
raw_dir = os.path.join("/home/lbybee/Dropbox/BK_LB_Projects",
                       "Text_Data_API/intermediate/DJ_newswire",
                       "unzipped_files")
count_dir = "/home/lbybee/scratch_local/DJN/counts"
table_dir = "/home/lbybee/scratch_local/DJN/tables"

# files
stop_word_files = ["config/DJN/stop_words",
                   "config/DJN/stop_words_DJN"]
regex_stop_word_files = ["config/DJN/regex_stop_words",
                         "config/DJN/regex_stop_words_DJN"]
log_file = "/home/lbybee/scratch_local/DJN/tables.log"
raw_files = os.listdir(raw_dir)

# cleaning kwds
term_count_kwds_dict = {}
term_count_kwds_dict["1gram"] = {"stop_word_files": stop_word_files,
                                 "regex_stop_word_files": regex_stop_word_files,
                                 "doc_lthresh": doc_lthresh_1gram,
                                 "stem": True}
term_count_kwds_dict["2gram"] = {"doc_lthresh": doc_lthresh_2gram}

# -------- #
# Run Code #
# -------- #

if __name__ == "__main__":

    table_wrapper(n, processes, txt_labels, raw_dir, count_dir, table_dir,
                  raw_files, log_file, term_count_kwds_dict,
                  table_file_processor, table_post_cleaner)
