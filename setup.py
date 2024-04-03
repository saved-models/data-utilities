import os
from setuptools import setup

from versioneer import get_cmdclass, get_versions

setup(
    version  = get_versions()['version'],
    cmdclass = get_cmdclass(),
)
