'''
utilities to build dataframe representations (ie DataFrameSchema)
 models for data input validation
'''

import pandera as pa
import pandas as pd
import json
import re
from urllib.request import urlopen

def get_dictionary(manifest_url):
    '''
    given a cdis manifest url pointing to a manifest json file,
    pull out the data dictionary (ie schema.json)

    using the manifest within the production repo ensures most updated 
    dictionary
    '''
    manifest_json = json.loads(urlopen(manifest_url).read())
    dictionary_url = manifest_json['global']['dictionary_url']
    dictionary = json.loads(urlopen(dictionary_url).read())
    return dictionary

class NodeDictionary:
    '''
    Goal here is make an object that contains a DataFrameSchema object in addition 
    to items that define each node (which help to (1) create the DataFrame Schema and
    (2) provide an easy way to look up node definitions)

    '''
    def __init__(self,manifest_url,type):
        self.type = type
        dictionary = get_dictionary(manifest_url) #note - full dictionary will be needed for node refs in future versions
        
        self.system_properties = dictionary[f'{type}.yaml']['systemProperties']
        self.properties = dictionary[f'{type}.yaml']['properties']
        self.links = dictionary[f'{type}.yaml']['links']
        self.required = dictionary[f'{type}.yaml']['required']
        self.unique_keys = dictionary[f'{type}.yaml']['uniqueKeys']

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
            allow_duplicates = False
        else:
            allow_duplicates = True 

        return allow_duplicates

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

    def find_reference_property(self):
        '''
        many properties point to a property within one of the default node yaml files
        such as _terms.yaml and _definition.yaml. This function (will)
        find all the properties associated with those yaml files
        '''
        #TODO: use $ref -- split on #/ -- and reference yaml file instead of hard coding
        pass

    def define_dtype(self,prop_name:str,prop_vals:dict):
        '''
        this function maps dtypes within schema yaml files and 
        returns the corresponding python type.

        enums are a special case -- within the gen3 dictionaries, 
        one can only have type or enum but not both. Therefore, the  type 
        needs to inferred from enum values. 

        Not sure if all enum values are read in as one type (ie either in 
        parsing from python or wtihin gen3. Therefore I assign an order).

        TODO: cols with $ref also will have types determined from other yaml files.
        Therefore, need to find the type for this using the find_reference_property
        fxn (to be created in future)
        '''
        #TODO: map all possible dtype values in yamls
        dtype_key = {
            'string':str,
            'integer':int,
            'number':float,
            'array':object
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
        #elif '$ref' in prop_vals and prop_name in links:
        elif 'submitter_id' in prop_name:
            dtype = str
        #TODO: in future may want to build this out but for now just skipping (eg id, create date)
        elif '$ref' in prop_vals: 
            return None
        else:
            raise Exception("No type in property...something is wrong with yaml file")
        return dtype

    def add_parent_prop_name(self,prop_name:str,parent_prop_name: str = 'submitter_id') -> str:
        '''
        change prop name if its a parent node property -- right now just submitter_id
        TODO: use $ref -- split on #/ -- and reference yaml file instead of hard coding
        '''
        if prop_name in [x['name'] for x in self.links]:
            prop_name = f"{prop_name}.{parent_prop_name}"
        return prop_name

    def build_column(self,prop_name,prop_vals):
        '''
        compile the arguments to a single Column or Index pandera classes
        '''
        prop_name = self.add_parent_prop_name(prop_name)
        column_args = {}
        column_args['dtype'] = self.define_dtype(prop_name,prop_vals)
        #determine checks needed. May want to add other custom for non-enum etc
        #TODO: make into general check fxn
        column_args['checks'] = []
        if 'enum' in prop_vals:
            enum_check = pa.Check.isin(prop_vals['enum'])
            column_args['checks'].append(enum_check)

        column_args['required'] = self.check_is_required(prop_name)
        column_args['allow_duplicates'] = self.check_require_unique(prop_name)
        return {prop_name:pa.Column(**column_args)}

    def build_schema(self) -> pa.DataFrameSchema:
        '''
        given a dictionary containing all the node jsons 
        (where the keys are yaml file names)
        and the desired node, build a pandera DataSchema object
        where all columns not in the DataFrameSchema are dropped 
        upon validation.
        '''
        links = {link['name']:link for link in self.links}
        pa_columns = {}
        for prop_name,prop_vals in self.properties.items():
            if prop_name not in self.system_properties:
                pa_columns.update(self.build_column(prop_name,prop_vals))
        schema = pa.DataFrameSchema(pa_columns,strict='filter')
        return schema


