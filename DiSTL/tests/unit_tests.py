from build_test import build_test_wrapper
import os

# build tests
directories = [os.path.join("test_data", "T%d" % d) for d in range(1, 11)]
for d in directories:
    build_test_wrapper(d)

# DTDF tests

# DDTDF tests
