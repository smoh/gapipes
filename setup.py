import io
import os
import re
import sys

from setuptools import find_packages
from setuptools import setup


if sys.version_info < (3, 6):
    sys.exit("Sorry, python < 3.6 is not supported")


def read(filename):
    filename = os.path.join(os.path.dirname(__file__), filename)
    text_type = type(u"")
    with io.open(filename, mode="r", encoding="utf-8") as fd:
        return re.sub(text_type(r":[a-z]+:`~?(.*?)`"), text_type(r"``\1``"), fd.read())


setup(
    name="gapipes",
    version="0.0.0",
    url="https://github.com/smoh/gapipes",
    license="MIT",
    author="Semyeong Oh",
    author_email="semyeong.oh@gmail.com",
    description="Lightweight collection of routines for fast exploration of Gaia+",
    long_description=read("README.md"),
    packages=find_packages(exclude=("tests",)),
    include_package_data=True,
    install_requires=[
        "pandas>=0.23",
        "requests",
        "astropy>=3",
        "beautifulsoup4>=4.6",
        "scipy",
    ],
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
)
