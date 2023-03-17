""" 
Generate and validate data package for submission by hubs
""" 
#get schemas and validate files, creating report,writing datasets, and package metadata
import os
from pathlib import Path
from jdc_utils import schema
from jdc_utils.encoding import core_measures as encodings
from jdc_utils.transforms.curation import to_new_names
from jdc_utils.transforms.deidentify import replace_ids,shift_dates
#frictionless
from frictionless import Package,Resource
from frictionless import transform,validate
# frictionless plugins
from dataforge.frictionless import encode_table
import dataforge.frictionless
from collections import abc
import re
import pandas as pd
import copy 
from abc import ABC,abstractmethod
from functools import reduce 


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

        # resolve paths just in case directories change
         # note, check that this is a directory in case the input
         # is something else like a url that is valid input to Package
        # NOTE: should all be pathlike
        def _resolve_if_path(var):
            if var:
                if os.path.isdir(var) or os.path.isfile(var):
                    return str(Path(var).resolve()) 
                else:
                    return var
            else:
                return var     

        self.filepath = _resolve_if_path(filepath)
        self.id_file = _resolve_if_path(id_file)
        self.id_column = id_column
        self.history_path = _resolve_if_path(history_path)
        self.date_columns = date_columns
        self.outdir = _resolve_if_path(outdir)
        self.filename = Path(filepath).name
        
        self.package = None 
        self.sourcepackage = None

        self.basedir = os.getcwd()

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
        package_pandas = Package(**kwargs)
        for resource in package.resources:
            try:
                name = resource.name
                data = resource.to_petl().todf()
                resource_pandas = Resource(data,name=name)
                package_pandas.add_resource(resource_pandas)
            except:
                print(f"In {filepath}")
                print(f"Something went wrong when loading {name}")
                print(f"Removing {name} from the source package")
                del resource
        
        os.chdir(pwd) #NOTE: change dir to base dir for other steps

        if is_core_measures:
            self.package = package_pandas 
            self.sync()
        else:
            self.sourcepackage = package_pandas
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

        os.chdir(self.basedir)

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

    def _to_new_names(self):
        fields = (
            schema.core_measures.baseline['fields'] + 
            schema.core_measures.timepoints['fields']
        )
        _mapnames = lambda x,y:{
            **{y['custom']['jcoin:original_name']:y['name'],
            **x}
        }
        mappings = reduce(_mapnames,fields,{})
        for resource in self.package.resources:
            sourcedf = resource.data.copy()
            targetdf = to_new_names(df=sourcedf,mappings=mappings)
            resource.data = targetdf
            resource.format = "pandas"

    def _add_missing_fields(self,missing_value="Missing"):


        for resource in self.package.resources:
            tbl = resource.to_petl()
            fieldnames = tbl.fieldnames()
            fields = []
            fields_to_add = []
            for field in resource.schema.fields:
                fields.append(field.name)
                if field.name not in fieldnames:
                    fields_to_add.append((field.name, missing_value))

            df = (
                tbl
                .addfields(fields_to_add)
                .cut(fields)
                .todf()
            )
            resource.data = df
            resource.format = "pandas"

    def sync(self):
        """ 
        if a core measure package exists
        sync by adding most up to date schemas and 
        adding missing fields.
        """ 
        self._to_new_names()
        self._add_schemas()
        self._add_missing_fields()
                
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

        Path(outdir).mkdir(exist_ok=True,parents=True)
        os.chdir(outdir)
        Path("schemas").mkdir(exist_ok=True)
        Path("data").mkdir(exist_ok=True)

        # write csv datasets and validation report
        for resource in self.package['resources']:
            csvpath = f"data/{resource['name']}.csv"
            schemapath = f"schemas/{resource['name']}.json"
            
            resource.schema.to_json(schemapath)
            resource.to_petl().tocsv(csvpath)

            self.written_package.add_resource(
                Resource(name=resource['name'],path=csvpath,schema=schemapath)
            )
        
        self.written_package.to_json(f"data-package.json")
        self.written_package_report = validate("data-package.json")
        self.written_package_report.to_json("report.json")
        Path("report-summary.txt").write_text(self.written_package_report.to_summary())


        # write SPSS/Stata files if valid package
        if self.written_package_report['valid']:
            ## copy package so trgt pckgs not added to iterator
            sourcepackage = self.written_package.to_copy()
            for source in sourcepackage['resources']:
                source_path = Path(source['path'])

                target_spss_path = f"data/{source['name']}.sav"
                target_stata_path = f"data/{source['name']}.dta"
                target_spss_schemapath = f"schemas/{source['name']}-sav.json"
                target_stata_schemapath = f"schemas/{source['name']}-dta.json"
                
                #TODO: test to see if instantiating new Resource is needed (may not be given the iterator is now copied)
                target_spss = Resource(path=str(source.path),schema = dict(source.schema))
                target_spss = target_spss.transform(steps=[encode_table(encodings=encodings.fields,
                    reservecodes=encodings.reserve['spss'])])
                #TODO: test to see if instantiating new Resource is needed (may not be given the iterator is now copied)
                target_stata = Resource(path=str(source.path),schema = dict(source.schema))
                target_stata = target_stata.transform(steps=[encode_table(encodings=encodings.fields,
                    reservecodes=encodings.reserve['stata'])])
                
                target_spss.infer()
                target_stata.infer()

                target_spss.write(target_spss_path)
                target_stata.write(target_stata_path)
                target_spss.schema.to_json(target_spss_schemapath)
                target_stata.schema.to_json(target_stata_schemapath)

                target_resource_spss = Resource(
                    name=f"{source['name']}-sav",
                    title="SPSS (.sav) dataset",
                    description="This is an annotated SPSS dataset. To see the schema with the value labels (encoding) and variable labels (title), see `schemas/<tablename>-sav.json`",
                    path=target_spss_path,
                    # schema=target_spss_schemapath #No validation/read stream yet (need to add to dataforge)
                )
                target_resource_stata = Resource(
                    name=f"{source['name']}-dta",
                    title="Stata (.dta) dataset",
                    description="This is an annotated Stata dataset. To see the schema with the value labels (encoding) and variable labels (title), see `schemas/<tablename>-sav.json`",
                    path=target_stata_path,
                    # schema=target_stata_schemapath #No validation/read stream yet (need to add to dataforge)
                )
                self.written_package.add_resource(target_resource_spss)
                self.written_package.add_resource(target_resource_stata)

            self.written_package.to_json("data-package.json")


        else:
            print(f"Package not valid so not generating spss and stata files. Check report summary")

        os.chdir(self.basedir)

        return self





        



            
            
            



