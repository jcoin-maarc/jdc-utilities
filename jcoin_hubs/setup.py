from setuptools import setup, find_namespace_packages
exec(open('../version.py').read())

setup(
    name='jcoin-hubs',
    version=__version__,
    author='JCOIN Methodology and Advanced Analytics Resource Center (MAARC)',
    description='Hub-specific code for preparing submissions to JDC',
    url='https://rcg.bsd.uchicago.edu/gitlab/maarc/dasc/jdc-utilities',
    packages=find_namespace_packages(where='../jcoin_hubs'),
    install_requires=[
        'jdc-utils',
    ],
)
