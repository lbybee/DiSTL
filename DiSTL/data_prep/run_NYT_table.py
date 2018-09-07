from NYT_methods_table import table_file_processor
from global_methods_table import table_wrapper
import os

# ---------- #
# Parameters #
# ---------- #

# misc params
n = 2
processes = 48
txt_labels = ["lead_paragraph", "abstract", "snippet"]
doc_lthresh_1gram = 10
doc_lthresh_2gram = 10

# directories
raw_dir = os.path.join("/home/lbybee/Dropbox/BK_LB_Projects",
                       "Text_Data_API/raw/NYT")
count_dir = "/home/lbybee/scratch_local/NYT/counts"
table_dir = "/home/lbybee/scratch_local/NYT/tables"

# files
stop_word_files = ["config/NYT/stop_words"]
regex_stop_word_files = ["config/NYT/regex_stop_words"]
log_file = "/home/lbybee/scratch_local/NYT/tables.log"
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
                  table_file_processor)
