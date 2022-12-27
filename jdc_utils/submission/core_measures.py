""" 
Generate and validate data package for submission by hubs
""" 
#get schemas and validate files, creating report,writing datasets, and package metadata
import os
from pathlib import Path
from jdc_utils import schema
from frictionless import Package,Resource
from frictionless import transform,validate
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
    """ 

    def __init__(self,filepath):
        self.filepath = filepath
        
        source = Package(filepath)
        target = Package()

        for resource in source.resources:
            name = (
                resource.name.lower()
                .replace("-","")
                .replace("_","")
            )
            if name in list(schemas):
                resource['schema'] = schemas[name]
                target.add_resource(resource)
    
        self.package = transform(target,steps=[add_missing_fields(missing_value='Missing')])
        
        
    
    def validate(self,outdir='',write_to_file=False):
        self.report = validate(self.package)
        
        return write_package_report(
            self.package,outdir,write_to_file
        )

    def write(self,outdir=''):
        #TODO: provide input for other study level info like 
        # description etc
        self.written_package = Package()

        for resource in self.package['resources']:
            csvpath = f"{outdir}/data/{resource['name']}.csv"
            schemapath = f"{outdir}/schemas/{resource['name']}.json"
            
            resource.schema.to_json(schemapath)
            resource.to_petl().tocsv(csvpath)

            self.written_package.add_resource(Resource(path=csvpath,schema=schemapath))
        
        self.written_package.to_json(f"{outdir}/data-package.json")


        



            
            
            



