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
        - path to a package directory (either containing a dat-package.json or core measure data files)
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
        self.filename = Path(filepath).name


        if "*" in self.filename:
            os.chdir(Path(filepath).parent)
            source = Package(self.filename,**kwargs)
        elif Path(filepath).is_dir():
            os.chdir(filepath)
            if Path('data-package.json').is_file():
                source = Package("data-package.json",**kwargs)
            elif Path('datapackage.json').is_file():
                source = Package("datapackage.json",**kwargs)
            else:
                source = Package("*",**kwargs)
        for resource in source.resources:
            resource.data = resource.to_pandas()
            resource.format = "pandas"

        self.package = source

    def deidentify(self,id_file=None, id_column=None,
        history_path=None, date_columns=None,
        fxns=["replace_ids","shift_dates"]):
        
        self.id_file = getattr(self,'id_file',id_file)
        self.id_column = getattr(self,'id_column',id_column)
        self.history_path = getattr(self, 'history_path',history_path)
        self.date_columns = getattr(self, 'date_columns',date_columns)

        for resource in self.package.resources:

            sourcedf = resource.data.copy()
            
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
    
    def add_schemas(self):
        # add schema 
        for resource in self.package.resources:
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
        
    def write(self,outdir='',**kwargs):
        """
         writes package to core measure format
         NOTE: use kwargs to pass in all package (ie hub)
         specific package properties (title,name,desc etc)
        """
        self.add_schemas()
        self.written_package = Package(**kwargs)
        os.chdir(outdir)
        for resource in self.package['resources']:
            csvpath = f"data/{resource['name']}.csv"
            schemapath = f"schemas/{resource['name']}.json"
            
            resource.schema.to_json(schemapath)
            resource.to_petl().tocsv(csvpath)

            self.written_package.add_resource(Resource(path=csvpath,schema=schemapath))
        
        self.written_package.to_json(f"data-package.json")
        self.written_package_report = validate("data-package.json")
        self.written_package_report.to_json("report.json")
        self.written_package_report.to_summary("report-summary.txt")





        



            
            
            



