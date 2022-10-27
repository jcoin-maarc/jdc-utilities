"""Generate TSV files for submission to JDC"""
from pathlib import Path
import os
import sys

import pandera as pa
import pandas as pd
import json
import re


from io import StringIO
import hashlib
import requests
import numpy as np

import yaml
from jsonpath_ng import parse
import json 

from collections.abc import Iterable

import glob

from frictionless import Schema,Resource,Detector
from frictionless import validate_resource,validate_package
#submission validation utilities for frictionless schemas and packaging


#can validate a package or resource -- starting with resource for hubs  to keep simple
# that is, if their file conforms to schema, then they can submit to box for packaging.
def build_package(resourcepaths):
    ''' 
    builds a package from tuple of resources in form 
    of (schema,resources)
    '''
    package = Package()
    #resource = Resource(resourcepath,schema=schema,detector=detector)
    for schemapath,datapath in resourcepaths:
        #detector = Detector(schema_sync=True) #Phil recommended to not use schema sync but rather get cols manually
        schema = Schema(schemapath)
        resource = Resource(datapath,schema=schema)
        package.add_resource(resource)

    #return messages
    return package

def create_package_validation_report(package):
    report = validate_package(package)
    messages = [v['message'] for v in report['errors']]
    return messages


def build_resource(schemapath,resourcepath):
    ''' 
    builds a resource from a schema and list of resources
    or string of a resource
    '''
    #detector = Detector(schema_sync=True) #if column missing will still say its valid
    schema = Schema(schemapath)
    #resource = Resource(resourcepath,schema=schema,detector=detector)
    resource = Resource(resourcepath,schema=schema)
    return resource 

def create_resource_validation_report(resource):
    report = validate_resource(resource)
    file_path_exp = parse("$..resource.path[*]")
    file_paths = [x.value for x in file_path_exp.find(report)]

    file_names = " and ".join([os.path.split(f)[-1] for f in file_paths])
    #errors = [task['errors'][0] for task in report['tasks'] if not report['valid']]
    errors_df = pd.DataFrame(
                report.flatten(["code", "rowPosition", "message", "description"]),
                columns=[
                    "error-category",
                    "row-number",
                    "error-message",
                    "general-error-description",
                ],
            )
    return {'file_names':file_names,'errors_df':errors_df,"is_valid":report['valid']}







# submission validation utilities for gen3 sheepdog validation 
def get_dictionary(endpoint,node_type='_all'):
    ''' 
    get json dictionary with all references (eg if $ref then API has these properties of this ref)
    
    _all gets entire schema while node names get only that node type
    ''' 
    api_url = f"{endpoint}/api/v0/submission/_dictionary/{node_type}"
    output = requests.get(api_url).text
    data = json.loads(output)
    return data

class Node:
    '''
    Goal here is make an object that contains a DataFrameSchema object in addition 
    to items that define each node (which help to (1) create the DataFrame Schema and
    (2) provide an easy way to look up node definitions)

    '''
    def __init__(self,endpoint,type):
        self.type = type
        dictionary = get_dictionary(endpoint,type) #note - full dictionary will be needed for node refs in future versions
        
        self.system_properties = dictionary['systemProperties']
        self.properties = dictionary['properties']
        self.links = dictionary['links']
        self.required = dictionary['required']
        self.unique_keys = dictionary['uniqueKeys']
        #get just the link names
        self.link_names = [x.value for x in parse("$..name").find(self.links)]

        self.schema = self.build_schema()

    def check_require_unique(self,prop_name:str):
        '''
        check whether a column/property can allow duplicates/required to be 
        uniuqe

        TODO:
        #check if prop is in uniquekeys after taking out system properties
        #submitter ids need to be unique (both from parent or current node). 
        #Put other columns that need to be unique here
        # allow_duplicates will be deprecated in future pandera (so change to unique=is_unique)
        '''

        if prop_name=='submitter_id':
            is_unique = True
        else:
            is_unique = False 

        return is_unique

    def check_is_required(self,prop_name:str):
        '''
        determine if property is required
        if link is required, then property must be required...I believe

        in case check is done after adding suffix(if parent node)
        '''
        prop_name = re.sub('\..*','',prop_name)
        if prop_name in self.required: 
            is_required = True
        elif prop_name in [x['name'] for x in self.links]:
            is_required = self.links[prop_name]['required']
        else:
            is_required = False
        
        return is_required

    def define_dtype(self,prop_name:str,prop_vals:dict):
        '''
        this function maps dtypes within schema yaml files and 
        returns the corresponding python type.

        enums are a special case -- within the gen3 dictionaries, 
        one can only have type or enum but not both. Therefore, the  type 
        needs to inferred from enum values. 

        Not sure if all enum values are read in as one type (ie either in 
        parsing from python or wtihin gen3. Therefore I assign an order).
        '''
        #TODO: map all possible dtype values in yamls
        dtype_key = {
            'string':str,
            'integer':int,
            'number':float,
            'array':object,
            'boolean':bool,
        }
        #determine dtype
        if 'type' in prop_vals:
            if type(prop_vals['type']) is str:
                dtype = dtype_key[prop_vals['type']]
            else: #type can be str or list
                dtype = dtype_key[prop_vals['type'][0]]
        elif 'enum' in prop_vals:
            enum_dtypes = set(type(x) for x in prop_vals['enum'])
            if str in enum_dtypes:
                dtype = str 
            elif float in enum_dtype:
                dtype = float 
            elif int in enum_dtype:
                dtype = int
            else: #TODO: add other possibilities? raise error as in current validate fxn?
                dtype = object
        elif 'oneOf' in prop_vals:
                dtype = object #NOTE: short-term solution
        else:
            raise Exception("No type in property...something is wrong with yaml file")
        return dtype

    def build_column(self,prop_name,prop_vals):
        '''
        compile the arguments to a single Column or Index pandera classes

        if the property is specified as an index, makes the column an Index object
        instead of a Column object (currently -- this is submitter_id)
        '''
        column_args = {}
        column_args['name'] = prop_name
        column_args['dtype'] = self.define_dtype(prop_name,prop_vals)
        column_args['coerce'] = True
        #determine checks needed. May want to add other custom for non-enum etc
        #TODO: make into general check fxn
        column_args['checks'] = []
        if 'enum' in prop_vals:
            enum_check = pa.Check.isin(prop_vals['enum'])
            column_args['checks'].append(enum_check)
            
        column_args['required'] = self.check_is_required(prop_name)
        #column_args['allow_duplicates'] = self.check_require_unique(prop_name)
        column_args['unique'] = self.check_require_unique(prop_name)

        if self.index_name==prop_name:
            del column_args['required'] #Index does not have this argument
            return pa.Index(**column_args)
        else:
            return {prop_name:pa.Column(**column_args)}

    def build_schema(self,index_name='submitter_id') -> pa.DataFrameSchema:
        '''
        given a dictionary containing all the node jsons 
        (where the keys are yaml file names)
        and the desired node, build a pandera DataSchema object
        where all columns not in the DataFrameSchema are dropped 
        upon validation.
        '''
        self.index_name = index_name
        #links = {link['name']:link for link in self.links}
        pa_columns = {}
        for prop_name,prop_vals in self.properties.items():
            if prop_name not in self.system_properties:
                if prop_name==index_name:
                    pa_index = self.build_column(prop_name,prop_vals)
                else:
                    #if property is a link
                    ##add parent node name and re-assign prop_name 
                    if prop_name in self.link_names:
                        prop_name = f"{prop_name}.submitter_id"
                        prop_vals = dict(self.properties['submitter_id'])
                    
                    pa_columns.update(self.build_column(prop_name,prop_vals))

        schema = pa.DataFrameSchema(columns=pa_columns,index=pa_index,strict='filter')
        return schema

    def to_tsv(
        self,
        df,
        file_dir='',file_name='',
        index=True,
        return_type=None
        ):
        ''' 
        outputs a tsv (or Node instance with validated dataframe if specified) 
        after (1) adding required node specific properties
        and (2) validating fields of node in the given dataframe

        '''
        data = df.copy()
        sep = '\t'
        #add node specific fields

        ##add the node type (ie name)
        if 'type' not in df:
            data['type'] = self.type

        #make submitter_id index if not already index
        if data.index.name!=self.index_name and self.index_name in data.columns:
            data.set_index(self.index_name,inplace=True)

        self.validated_data = self.schema.validate(data)
        
        if file_name:
            Path(file_dir).mkdir( parents=True, exist_ok=True )
            self.validated_data.to_csv(os.path.join(file_dir,file_name),index=index,sep='\t')

        if return_type=='node':
            return self
        elif return_type=='tsv':
            return self.validated_data.to_csv(index=index,sep=sep)
        else:
            pass

