from DJ_methods_count import unigram_tag_file_processor
from global_methods_count import unigram_tag_wrapper
import os


# ---------- #
# Parameters #
# ---------- #

# misc params
processes = 48
txt_labels = ["body", "headline"]

# directories
in_data_dir = os.path.join("/home/lbybee/Dropbox/BK_LB_Projects",
                           "Text_Data_API/intermediate/DJ_newswire",
                           "unzipped_files")
tmp_dir = "/home/lbybee/scratch_local/DJN/raw_counts"
out_count_dir = "/home/lbybee/scratch_local/DJN/counts"

# files
log_file = "/home/lbybee/scratch_local/DJN/base_counts.log"
raw_files = os.listdir(in_data_dir)

# term aggregates
term_columns = ["term_label", "doc_count", "term_count"]
term_groupby_col = "term_label"
term_index_name = "term_id"

# tag aggregates
tag_columns = ["tag_label", "doc_count"]
tag_groupby_col = "tag_label"
tag_index_name = "tag_id"


# -------- #
# Run Code #
# -------- #

if __name__ == "__main__":

    unigram_tag_wrapper(processes, in_data_dir, tmp_dir, out_count_dir,
                        log_file, raw_files, txt_labels, term_columns,
                        term_groupby_col, term_index_name, tag_columns,
                        tag_groupby_col, tag_index_name,
                        unigram_tag_file_processor)
