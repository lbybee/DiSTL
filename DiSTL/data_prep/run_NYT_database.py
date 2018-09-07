from global_methods_database import database_wrapper
from NYT_methods_database import write_sql
import os

# ---------- #
# Parameters #
# ---------- #

# db params
db = "test"
schema = "NYT"

# misc params
n = 2
processes = 48

# directories
raw_dir = os.path.join("/home/lbybee/Dropbox/BK_LB_Projects",
                       "Text_Data_API/raw/NYT")
count_dir = "/home/lbybee/scratch_local/NYT/counts"
table_dir = "/home/lbybee/scratch_local/NYT/tables"
sql_dir = "/home/lbybee/scratch_local/NYT/sql"

# files
raw_files = os.listdir(raw_dir)
log_file = "/home/lbybee/scratch_local/NYT/database.log"

# labels
txt_labels = ["lead_paragraph", "abstract", "snippet"]
partitions = [f.replace(".json", "") for f in raw_files]
ngram_labels = ["%dgram" % d for d in range(1, n + 1)]
term_labels = ["term_%s" % l for l in ngram_labels]
tags = [f.replace(".csv", "") for f in os.listdir(count_dir)]
tags = [l for l in tags if l not in ["%s_%s" % (t, n)
                                           for t in txt_labels
                                           for n in ngram_labels]]
link_labels = ["tag_link_%s" % l for l in tags]
tag_labels = ["tag_%s" % l for l in tags]
count_labels = ["count_%s_%s" % (t, n) for t in txt_labels
                for n in ngram_labels]



# -------- #
# Run Code #
# -------- #

if __name__ == "__main__":

    write_sql(partitions, ngram_labels, txt_labels, tags, sql_dir)
    database_wrapper(db, schema, processes, term_labels, tag_labels,
                     count_labels, link_labels, partitions,
                     table_dir, sql_dir, log_file)
