"""Minimal setup.py used only to mark the wheel as a binary distribution.

All metadata lives in ``pyproject.toml``. The bundled
``libdataframe-arrow.dylib`` (or ``.so``) is platform-specific but does NOT
link against libpython, so the wheel is Python-version-agnostic — we set
the wheel tag to ``py3-none-<platform>`` accordingly.
"""

from setuptools import setup
from setuptools.dist import Distribution

try:
    from wheel.bdist_wheel import bdist_wheel as _bdist_wheel
except ImportError:  # pragma: no cover
    _bdist_wheel = None


class BinaryDistribution(Distribution):
    def has_ext_modules(self):
        return True

    def is_pure(self):
        return False


cmdclass = {}

if _bdist_wheel is not None:

    class bdist_wheel(_bdist_wheel):
        def finalize_options(self):
            _bdist_wheel.finalize_options(self)
            self.root_is_pure = False

        def get_tag(self):
            python, abi, plat = _bdist_wheel.get_tag(self)
            # The native lib is platform-specific but doesn't link libpython,
            # so the wheel works on any Python 3 ABI on the same platform.
            return "py3", "none", plat

    cmdclass["bdist_wheel"] = bdist_wheel


setup(distclass=BinaryDistribution, cmdclass=cmdclass)
