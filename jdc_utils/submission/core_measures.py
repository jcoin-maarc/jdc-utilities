""" 
Generate and validate data package for submission by hubs
""" 
#get schemas and validate files, creating report,writing datasets, and package metadata
import os
from jdc_utils import schema
from frictionless import Package,Resource
from frictionless import validate
from collections import abc
from attrs import asdict
from dataforge.frictionless import add_missing_fields,write_package_report

def _add_resources_missing_fields(package):
    target = transform(
        package,
        steps=[
                steps.resource_transform(
                    name=r['name'], steps=[add_missing_fields('Missing')])
            for r in resource_names
        ]
    )       
    return target

def get_package(filepath,do_field_add=True):
    """ 
    gets a package containing the paths to resources (filepath) 
    
    Paramaters
    --------------
    filepath: can be one of:
        - a path to a data file
        - path to a glob-like regular expression for multiple data files
        - can also be a package descriptor file (eg data-package.json) with resources
        (technically can also be a Package object in addition to a file path)
    do_field_add: flag to indicate whether to add missing fields and track in metadata.

    Returns
    --------------
    package with schema added and added missing values (in resource metadata and actual table)
    """ 
    package = Package(filepath)
    for resource in package['resources']:

        if not resource.get('name'):
            resource['name'] = name = Path(resource['path']).stem.lower()

        assert resource['name'] in var(sche)

        if not resource.get('schema'):
            #convert Simplenamespace to dictionary to reference schema based on filename stem
            resource['schema'] = var(schema.core_measures)[name.replace("-","")]
    
    if do_field_add:
        package = _add_resources_missing_fields(package)
    return package






