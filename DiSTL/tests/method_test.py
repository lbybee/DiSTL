# add path to code
import sys
sys.path.append("../")

# imports
from core import loadDTDF

dtdf = loadDTDF("test_data/T1/DTDF", distributed=1)
