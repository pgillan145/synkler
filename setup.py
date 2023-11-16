#!/usr/bin/env python3

from setuptools import find_packages, setup
import synkler

with open("./README.md", encoding="utf-8") as f:
    readme = f.read()

setup(
    name='synkler',
    description="A three-body rsync solution.",
    packages=find_packages(include=['synkler']),
    author="Patrick Gillan",
    author_email = "pgillan@minorimpact.com",
    entry_points = { "console_scripts": [ "synkler = synkler:main" ] },
    install_requires=['minorimpact', 'pika', 'psutil'],
    license='GPLv3',
    long_description = readme,
    long_description_content_type = "text/markdown",
    setup_requires=[],
    tests_require=[],
    url="https://github.com/pgillan145/synkler",
    version=synkler.__version__
)
