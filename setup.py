from setuptools import setup, find_namespace_packages
from pathlib import Path
from version import __version__
requirements = Path("requirements.txt").open().readlines()
setup(
    name='jdc-utils',
    version=__version__,
    author='Michael Kranz,Phil Schumm',
    author_email='kranz-michael@norc.org,pschumm@uchicago.edu',
    description='Utilities for preparing and submitting data to the JDC',
    url='https://github.com/jcoin-maarc/jdc-utilities',
    packages=find_namespace_packages(where='.'),
    install_requires=requirements,
    entry_points='''
        [console_scripts]
        jdc-utils=jdc_utils.cli:cli
    ''',
)
