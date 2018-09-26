"""
test runner
"""
from DiSTL.utilities import runner
import os


# ------------- #
# CV Parameters #
# ------------- #

# -------- #
# Run Code #
# -------- #

def test_method():

    print("foo bar")

state_file = "test.json"

task_list = []

task_list = [{"label": "test",
              "method": test_method,
              "method_kwds": {},
              "dependencies": []}]

method_wrapper_kwds = {"email_kwds": {"log": "hello world"}}

runner(task_list, state_file, method_wrapper_kwds=method_wrapper_kwds)
