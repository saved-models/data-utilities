#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="fisdat",
    version="0.2",
    description="Data schema checking and uploading",
    author=["William Waites", "Meadhbh Moriarty", "Duncan Guthrie"],
    author_email="william.waites@strath.ac.uk",
    keywords=["biological data", "semantic web"],
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        "Development Status :: 3 - Alpha",
        # Intended audience
        "Intended Audience :: Education",
        "Intended Audience :: Science/Research",
        # License
        #'License :: OSI Approved :: GNU General Public License (GPL)',
        # Specify the Python versions you support here. In particular,
        # ensure that you indicate whether you support Python 2, Python 3
        # or both.
        "Programming Language :: Python :: 3",
    ],
    license="GPLv3",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "google-cloud-storage",
        "linkml==1.7.10",
        "linkml-runtime==1.7.5",
        "mypy"
    ],
    entry_points={
        "console_scripts": [
            "fisdat = fisdat.cmd_dat:cli",
            "fisup = fisdat.cmd_up:cli",
        ],
    },
)
