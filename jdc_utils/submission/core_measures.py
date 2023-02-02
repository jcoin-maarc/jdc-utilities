""" 
Generate and validate data package for submission by hubs
""" 
#get schemas and validate files, creating report,writing datasets, and package metadata
import os
from pathlib import Path
from jdc_utils import schema
from jdc_utils.transforms.deidentify import replace_ids,shift_dates
from frictionless import Package,Resource
from frictionless import transform,validate
from collections import abc
from dataforge.frictionless import add_missing_fields,write_package_report
import re


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
    id_file (optional): the generated ids (see replace_id function for usage)
    id_column (optional): id column(s) for deidentification fxns (see replace ids and shift date fxns)
    history_path (optional): directory containing all version control history of mapping files (ie git bare repos)
    date_columns (optional): the specified date columns for shift dates function 
        (if none will default to all date col types in df. if no date col types, then will not convert anything
    outdir (optional): directory to write core measure package
    
    """ 

    def __init__(self,filepath,
        id_file=None,id_column=None,history_path=None,
        date_columns=None,outdir=None,**kwargs):

        self.filepath = filepath
        self.id_file = id_file
        self.id_column = id_column
        self.history_path = history_path
        self.date_columns = date_columns
        self.outdir = outdir
        
        source = Package(filepath,**kwargs)
        target = Package()

        for resource in source.resources:
            name = (
                resource.name.lower()
                .replace("-","")
                .replace("_","")
            )

            #in case local files have prefixes etc
            for s in schemas:
                match = re.search(s,name)

            if match:
                resource['schema'] = schemas[match.group()]
                target.add_resource(resource)
    
        self.package = transform(target,steps=[add_missing_fields(missing_value='Missing')])

    def deidentify(self,id_file=None, id_column=None,
        history_path=None, date_columns=None,
        fxns=["replace_ids","shift_dates"]):
        
        self.id_file = getattr(self,'id_file',id_file)
        self.id_column = getattr(self,'id_column',id_column)
        self.history_path = getattr(self, 'history_path',history_path)
        self.date_columns = getattr(self, 'date_columns',date_columns)

        for resource in self.package.resources:

            sourcedf = resource.to_pandas()
            
            if "replace_ids" in fxns:
                sourcedf = replace_ids(sourcedf,
                        id_file=self.id_file,
                        id_column=self.id_column,
                        history_path=self.history_path
                    )
            if "shift_dates" in fxns:
                sourcedf = shift_dates(sourcedf,
                        id_column=self.id_column,
                        date_columns=self.date_columns,
                        history_path=self.history_path)

            resource.data = sourcedf
            resource.format = "pandas"
            
    def validate(self,outdir='',write_to_file=False):
        self.report = validate(self.package)
        
        return write_package_report(
            self.package,outdir,write_to_file
        )

    def write(self,outdir='',**kwargs):
        """
         writes package to core measure format
         NOTE: use kwargs to pass in all package (ie hub)
         specific package properties (title,name,desc etc)
        """
        self.written_package = Package(**kwargs)

        for resource in self.package['resources']:
            csvpath = f"{outdir}/data/{resource['name']}.csv"
            schemapath = f"{outdir}/schemas/{resource['name']}.json"
            
            resource.schema.to_json(schemapath)
            resource.to_petl().tocsv(csvpath)

            self.written_package.add_resource(Resource(path=csvpath,schema=schemapath))
        
        self.written_package.to_json(f"{outdir}/data-package.json")


        



            
            
            



