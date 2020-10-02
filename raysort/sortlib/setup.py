from setuptools import Extension, setup
from Cython.Build import cythonize

setup(
    ext_modules=cythonize(
        [Extension("sortlib", ["sortlib.pyx"])],
        gdb_debug=True,
        compiler_directives={"language_level": "3"},
    )
)
