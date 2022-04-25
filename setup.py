from setuptools import setup, find_namespace_packages

setup(
    name='jdc-utils',
    version='0.0.1',
    author='Phil Schumm',
    author_email='pschumm@uchicago.edu',
    description='Utilities for preparing and submitting data to the JDC',
    url='https://rcg.bsd.uchicago.edu/gitlab/maarc/dasc/jdc-utilities',
    packages=find_namespace_packages(where='.'),
    package_data={'jdc_utils': ['frictionless/table_schemas/table_schema_urls.yaml']},
    install_requires=[
        'pandas',
        'pandas_flavor',
        'gitpython',
        'click',
        'openpyxl',
        'xlrd',
        'pyyaml',
        'pandera',
        'jsonpath_ng',
        'requests',
        'frictionless',
        'gen3'
    ],
    entry_points='''
        [console_scripts]
        jdc-utils=jdc_utils.cli:cli
    ''',
)
