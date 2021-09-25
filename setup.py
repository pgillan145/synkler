#!/usr/bin/env python3

from setuptools import find_packages, setup
import synkler

setup(
    name='synkler',
    description="A three-body rsync solution.",
    packages=find_packages(include=['synkler']),
    author="Patrick Gillan",
    author_email = "pgillan@minorimpact.com",
    entry_points = { "console_scripts": [ "synkler = synkler:main" ] },
    install_requires=['minorimpact', 'pika'],
    license='GPLv3',
    setup_requires=[],
    tests_require=[],
    url="https://github.com/pgillan145/synkler",
    version=synkler.__version__
)
