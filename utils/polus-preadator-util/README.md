# Preadator Utility

Streamlined management of threads and processes for algorithm scaling.

Supports Python>=3.9.

# Bugs

Preadator is designed to work in Python >= 3.7, but there is a 
[bug in Python](https://bugs.python.org/issue39098)
in the `ProcessPoolExecutor` that causes an `OSError` with
`.shutdown(wait=False)`.

Since the bug is in Python itself, currently only Python 3.9 is supported. If
the bug is fixed, then Preadator will support older versions of Python.

## Install

`pip install preadator`
