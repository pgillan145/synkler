from setuptools import find_packages, setup

setup(
    name='synkler',
    #packages=find_packages(include=['synkler']),
    version='0.0.1',
    description="A three-body rsync solution.",
    author="Patrick Gillan",
    author_email = "pgillan@minorimpact.com",
    license='GPLv3',
    install_requires=['minorimpact', 'pika'],
    setup_requires=[],
    tests_require=[],
    url="https://github.com/pgillan145/synkler"
)
