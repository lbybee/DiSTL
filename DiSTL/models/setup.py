from distutils.core import setup
from Cython.Build import cythonize
import numpy as np

setup(
    name="LDA_c_methods",
    ext_modules=cythonize("LDA_c_methods.pyx"),
    include_dirs=[np.get_include()]
)
