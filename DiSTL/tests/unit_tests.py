from build_test import build_test_wrapper
from datetime import datetime
import os

# build tests
directories = [os.path.join("test_data", "T%d" % d) for d in range(1, 11)]
t0 = datetime.now()
for i, d in enumerate(directories):
    t1 = datetime.now()
    build_test_wrapper(d)
    t2 = datetime.now()
    print(d, i, t2 - t0, t2 - t1)

# DTDF tests

# DDTDF tests
