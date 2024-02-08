#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="fisdat",
    version="0.1",
    description="Data schema checking and uploading",
    author=["William Waites", "Meadhbh Moriarty"],
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
    install_requires=[
        "rdflib",
        "csvwlib",
        "google-cloud-storage",
    ],
    entry_points={
        "console_scripts": [
            "fisdat = fisdat.cmd_dat:cli",
            "fisup = fisdat.cmd_up:cli",
        ],
    },
)
