""" 
Generate and validate data package for submission by hubs
""" 
#get schemas and validate files, creating report,writing datasets, and package metadata
import os
from pathlib import Path
from jdc_utils import schema
from frictionless import Package,Resource
from frictionless import validate
from collections import abc
from dataforge.frictionless import add_missing_fields,write_package_report



schemas = schema.core_measures.__dict__



class CoreMeasures:
    """ 
    object that takes in a path-like object pointing to data file(s)
    or anything accepted by the
    frictionless package object (eg a datapackage.json)
    
    package containing the paths to resources (filepath) 
    
    Paramaters
    --------------
    filepath: can be one of:
        - a path to a data file
        - path to a glob-like regular expression for multiple data files
        - can also be a package descriptor file (eg data-package.json) with resources
        (technically can also be a Package object in addition to a file path)
    Returns
    --------------
    package with schema added and added missing values (in resource metadata and actual table)
    """ 

    def __init__(self,filepath,schemas=None):
        #get package
        #validate
        #write if valid
        self.filepath = filepath
        self.package = transform(Package(filepath),steps=[add_missing_fields('Missing')])
        for resource in self.package['resources']:
            if not resource.get('name'):
                _add_name_from_path(resource)

            if not resource.get('schema'):
                _add_schema(resource)


    @staticmethod
    def _add_name_from_path(resource:Resource):
            #NOTE: the dash and underscores are moved
            #to support various versions of names used
            # such as time-points,time_points,and timepoints
            resource['name'] = name = (
                Path(resource['path']).stem
                .lower()
                .replace("-","")
                .replace("_","")
            )

    @staticmethod
    def _add_schema(resource:Resource):
        assert_msg = f"{resource['name']} must match one of {','.join(list(schemas))}"
        assert resource['name'] in list(schemas),assert_msg
        resource['schema'] = schemas[name]

    def validate(self,outdir='',):
        self.report = write_package_report(
            self.package,outdir
        )
        
    def write(self,outdir=''):
        self.written_package = self.package.copy()
        del self.written_package['resources']

        for resource in self.package['resources']:
            csvpath = f"{outdir}/data/{resource['name']}.csv"
            schemapath = f"{outdir}/schemas/{resource['name']}.json"
            
            resource.schema.to_json(schemapath)
            resource.to_petl().tocsv(csvpath)

            self.written_package.add_resource(Resource(path=csvpath,schema=schemapath))
        
        self.written_package.to_json(f"{outdir}/data-package.json")


        



            
            
            



