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
        'pandas==1.4.1',
        'pandas_flavor=0.2.0',
        'gitpython==3.1.27',
        'click==7.1.2',
        'openpyxl==3.0.9',
        'xlrd==2.0.1',
        'pyyaml==5.4.1',
        'pandera==0.9.0',
        'jsonpath_ng==1.5.3',
        'requests==2.27.1',
        'frictionless==4.26.0',
        'gen3==4.4.0'
    ],
    entry_points='''
        [console_scripts]
        jdc-utils=jdc_utils.cli:cli
    ''',
)
