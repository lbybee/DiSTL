from  setuptools import setup, find_packages, Extension, dist
from Cython.Build import cythonize
import numpy as np

ext_modules=[Extension("LDA_c_methods",
                       sources=["DiSTL/models/LDA_c_methods.pyx"],
                       include_dirs=[np.get_include()]),
              Extension("eLDA_c_methods",
                        sources=["DiSTL/models/eLDA_c_methods.pyx"],
                        include_dirs=[np.get_include()]),
#             Extension("parsers",
#                       sources=["DiSTL/parse/parsers.pyx"],
#                       include_dirs=[np.get_include()])
            ]

setup(name="DiSTL",
      packages=find_packages(),
      ext_modules = cythonize(ext_modules)
      )
