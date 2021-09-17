import pandera as pa
import pandas as pd
import json
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

def check_require_unique(prop_name:str,unique_keys:list,system_properties:list):
    #check if prop is in uniquekeys after taking out system properties
    #submitter ids need to be unique (both from parent or current node). 
    #Put other columns that need to be unique here
    # allow_duplicates will be deprecated in future pandera (so change to unique=is_unique)
    if 'submitter_id' in prop_name:
        allow_duplicates = False
    else:
        allow_duplicates = False 

    return allow_duplicates

def check_is_required(prop_name:str,links:dict,required:list):
    #determine if property is required
    #if link is required, then property must be required...I believe
    if prop_name in required: 
        is_required = True
    elif prop_name in links:
        is_required = links[prop_name]['required']
    else:
        is_required = False
    
    return is_required

def find_reference_property():
    #TODO: use $ref -- split on #/ -- and reference yaml file instead of hard coding
    pass

def define_dtype(prop_name:str,prop_vals:dict):
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

def check_enum(checks,prop_vals:dict):
    if 'enum' in prop_vals:
        checks.append(pa.Check.isin(prop_vals['enum']))

def add_parent_prop_name(prop_name:str,links:dict,parent_prop_name: str = 'submitter_id') -> str:
    #change prop name if its a parent node property -- right now just submitter_id
    #TODO: use $ref -- split on #/ -- and reference yaml file instead of hard coding
    if prop_name in links:
        prop_name = f"{prop_name}.{parent_prop_name}"
    return prop_name

def build_column(prop_name,prop_vals,links,required,unique_keys,system_properties):
    column_args = {}
    column_args['dtype'] = define_dtype(prop_name,prop_vals)
    #determine checks needed. May want to add other custom for non-enum etc
    column_args['checks'] = []
    check_enum(column_args['checks'],prop_vals)

    column_args['required'] = check_is_required(prop_name, links, required)
    column_args['allow_duplicates'] = check_require_unique(prop_name,unique_keys,system_properties)
    add_parent_prop_name(prop_name,links)
    return {prop_name:pa.Column(**column_args)}

def build_schema(
    properties:dict,
    links:dict,
    required:list[str],
    unique_keys:list[list],
    system_properties:list[str]
    ) -> pa.DataFrameSchema:
    '''
    given a dictionary containing all the node jsons 
    (where the keys are yaml file names)
    and the desired node, build a pandera DataSchema object
    '''
    links = {link['name']:link for link in links}
    pa_columns = {}
    for prop_name,prop_vals in properties.items():
        if prop_name not in system_properties:
            pa_columns.update(
                build_column(
                    prop_name,prop_vals,links,required,unique_keys,system_properties
                )
            )
    schema = pa.DataFrameSchema(pa_columns)
    return schema


