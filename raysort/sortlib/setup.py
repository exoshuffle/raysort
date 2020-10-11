from setuptools import Extension, setup
from Cython.Build import cythonize

setup(
    ext_modules=cythonize(
        [Extension("sortlib", ["sortlib.pyx"])],
        compiler_directives={"language_level": "3"},
    )
)
