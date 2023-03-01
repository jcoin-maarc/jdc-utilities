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
import pandas as pd
import copy 
from abc import ABC,abstractmethod
schemas = schema.core_measures.__dict__

class CoreMeasures:
    """ 
    Object that takes in a path-like object pointing to data file(s)
    or anything accepted by the frictionless package object
    (e.g., a datapackage.json) containing the paths to resources (filepath).
    
    Parameters
    ----------
    filepath: Union[str, Path]
        Can be one of the following:
            - A path to a data file
            - A path to a glob-like regular expression for multiple data files
            - A package descriptor file (e.g., data-package.json) with resources
            - A path to a package directory (either containing a data-package.json,
            core measure data files, or input files to be transformed into core measure files)
    id_file: Optional[str]
        The generated ids (see `replace_id` function for usage)
    id_column: Optional[str]
        ID column(s) for deidentification functions (see `replace_ids` and `shift_date` functions)
    history_path: Optional[str]
        Directory containing all version control history of mapping files 
        (i.e., git bare repos)
    date_columns: Optional[str]
        The specified date columns for `shift_dates` function 
        (if none, will default to all date column types in df. if no date column types, then will not convert anything)
    outdir: Optional[str]
        Directory to write core measure package
    is_core_measures: Optional[bool]
        Specifies whether the input is already a core measure package. 
        This may occur if there are the necessary files (i.e., baseline.csv and timepoints.csv) and only 
        packaging is required. For example, there may be a separate workflow that does the transformations.
    kwargs:
        Any other package properties you want to pass into the Package object
    """ 

    def __init__(
        self,
        filepath=None,
        id_file=None,
        id_column=None,
        history_path=None,
        date_columns=None,
        outdir=None,
        is_core_measures=False,
        **kwargs):

        self.filepath = filepath
        self.id_file = id_file
        self.id_column = id_column
        self.history_path = history_path
        self.date_columns = date_columns
        self.outdir = outdir
        self.filename = Path(filepath).name
        
        self.package = None 
        self.sourcepackage = None


        pwd = os.getcwd()
        filename = self.filename 
        filepath = self.filepath
        print(pwd)
        # NOTE for code below: frictionless security doesn't play well with particular paths
        # see: https://specs.frictionlessdata.io/data-resource/#data-location
        os.chdir(Path(filepath).parent)

        if Path(Path(filepath).name).is_dir():
            os.chdir(Path(filepath).name)
            if Path('data-package.json').is_file():
                package = Package("data-package.json",**kwargs)
            elif Path('datapackage.json').is_file():
                package = Package("datapackage.json",**kwargs)
            else:
                package = Package("*",**kwargs)
        else:
            package = Package(filename,**kwargs)

        print(os.getcwd())

        # has data package
        # has a baseline and timepoints resource

        for resource in package.resources:
            name = resource.name
            resource.data = resource.to_petl().todf()
            resource.format = "pandas"
            resource.name = name
        
        os.chdir(pwd) #NOTE: change dir to base dir for other steps

        if is_core_measures:
            self.package = package 
            self.sync()
        else:
            self.sourcepackage = package
            self.package = Package(**kwargs)
        
    def to_baseline():
        """ 
        takes the source package (self.sourcepackage) and, after the necessary transforms,
        adds the baseline resource
        to the core measure package in the format of a pandas dataframe (self.package).


        Note, if is_core_measure is specified -- for example if there are already 
        the appropriate data files, this function isn't necessary, which is why
        it is optional rather than required with @abstractmethod.

        """ 
        print("This is specific for each hub. Please define your specific function here.")
        return self
    def to_timepoints():
        """ 
        takes the source package (self.sourcepackage) and, after the necessary transforms,
        adds the timepoints resource
        to the core measure package in the format of a pandas dataframe (self.package).


        Note, if is_core_measure is specified -- for example if there are already 
        the appropriate data files, this function isn't necessary, which is why
        it is optional rather than required with @abstractmethod.

        """ 
        print("This is specific for each hub. Please define your specific function here.")
        return self

    def deidentify(self,id_file=None, id_column=None,
        history_path=None, date_columns=None,
        fxns=["replace_ids","shift_dates"]):
        """ apply deidentification steps to the source package
        """ 
        assert self.sourcepackage

        def _getattrcopy(varstr):
            return copy.copy(getattr(self,varstr,None))
              
        if id_file:
            setattr(self,'id_file',id_file)
        if id_column:
            setattr(self,'id_column',id_column)
        if history_path:
            setattr(self,'history_path',history_path)
        if date_columns:
            setattr(self,'date_columns',date_columns)
  
        for resource in self.sourcepackage.resources:
            id_file = _getattrcopy('id_file')
            id_column = _getattrcopy('id_column')
            history_path =  _getattrcopy('history_path')
            date_columns =  _getattrcopy('date_columns')

            sourcedf = resource.data.copy()
            
            if "replace_ids" in fxns:
                sourcedf = replace_ids(sourcedf,
                        id_file=id_file,
                        id_column=id_column,
                        history_path=history_path
                    )
            if "shift_dates" in fxns:
                if "replace_ids" in fxns:
                    id_column = pd.read_csv(self.id_file).squeeze().name 
                else:
                    id_column = _getattrcopy('id_column',id_column)
                sourcedf = shift_dates(sourcedf,
                        id_column=id_column,
                        date_columns=date_columns,
                        history_path=history_path)

            resource.data = sourcedf
            resource.format = "pandas"

        return self
            
    def _add_schemas(self):
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
    
    def sync(self):
        """ 
        if a core measure package exists
        sync by adding most up to date schemas and 
        adding missing fields.
        """ 

        self._add_schemas()
        self.package = transform(self.package,steps=[
                add_missing_fields(missing_value="Missing")])
                
    def write(self,outdir=None,**kwargs):
        """
         writes package to core measure format
         NOTE: use kwargs to pass in all package (ie hub)
         specific package properties (title,name,desc etc)
        """
        self.sync()

        self.written_package = Package(**kwargs)

        if self.outdir:
            outdir = self.outdir 
        else:
            self.outdir = outdir

        Path(outdir).mkdir(exist_ok=True)
        os.chdir(outdir)
        Path("schemas").mkdir(exist_ok=True)
        Path("data").mkdir(exist_ok=True)
        for resource in self.package['resources']:
            csvpath = f"data/{resource['name']}.csv"
            schemapath = f"schemas/{resource['name']}.json"
            
            resource.schema.to_json(schemapath)
            resource.to_petl().tocsv(csvpath)

            self.written_package.add_resource(Resource(path=csvpath,schema=schemapath))
        
        self.written_package.to_json(f"data-package.json")
        self.written_package_report = validate("data-package.json")
        self.written_package_report.to_json("report.json")
        Path("report-summary.txt").write_text(self.written_package_report.to_summary())
        return self





        



            
            
            



