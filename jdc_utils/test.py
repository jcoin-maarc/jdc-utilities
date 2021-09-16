import pandera as pa
import pandas as pd

from urllib.request import urlopen
import json


#manifest used for production deployment
MANIFEST_URL = 'https://raw.githubusercontent.com/uc-cdis/cdis-manifest/master/jcoin.datacommons.io/manifest.json'
manifest_json = json.loads(urlopen(MANIFEST_URL).read())
dictionary_url = manifest_json['global']['dictionary_url']
dictionary = json.loads(urlopen(dictionary_url).read())
type_json = dictionary['demographic.yaml']

#get required list
required = [r for r in type_json['required'] if r!='type']
links = {link['name']:link for link in type_json['links']}
pa_columns = {}
properties = type_json['properties']
for prop_name,prop_vals in properties.items():
    column_args = {}
    #determine if property is required
    if prop_name in required: 
        column_args['required'] = True
    elif prop_name in links:
        if links[prop_name]['required']:
            column_args['required'] = True
    else:
        column_args['required'] = False

    #TODO: map all possible dtype values in yamls
    dtype_key = {
        'string':str,
        'integer':int,
        'number':float
    }
    #determine dtype
    if 'type' in prop_vals:
        if type(prop_vals['type']) is str:
            dtype = dtype_key[prop_vals['type']]
        else: #type can be str or list
            dtype = dtype_key[prop_vals['type'][0]]
        column_args['dtype'] = dtype
    elif 'enum' in prop_vals:
        enum_dtypes = set(type(x) for x in prop_vals['enum'])
        if str in enum_dtypes:
            column_args['dtype'] = str 
        elif float in enum_dtype:
            column_args['dtype'] = float 
        elif int in enum_dtype:
            column_args['dtype'] = int
        else: #TODO: add other possibilities? raise error as in current validate fxn?
            column_args['dtype'] = object
    elif '$ref' in prop_vals and prop_name in links:
        column_args['dtype'] = str
    #TODO: in future may want to build this out but for now just skipping (eg id, create date)
    elif '$ref' in prop_vals: 
        continue
    else:
        raise Exception("No type in property...something is wrong with yaml file")

    #submitter ids need to be unique (both from parent or current node). Put other columns that need to be unique here
    if 'submitter_id' in prop_name:
        column_args['allow_duplicates'] = True #future pandera version will be  kwarg "unique"
    #determine checks needed. May want to add other custom for non-enum etc
    column_args['checks'] = checks = []
    if 'enum' in prop_vals:
        checks.append(pa.Check.isin(prop_vals['enum']))

    #change prop name if its a parent node property -- right now just submitter_id
    #TODO: use $ref -- split on #/ -- and reference yaml file instead of hard coding
    if prop_name in links:
        prop_name = f"{prop_name}.submitter_id"
    pa_columns.update({prop_name:pa.Column(**column_args)})
schema = pa.DataFrameSchema(pa_columns)





