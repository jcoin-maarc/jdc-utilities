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

def check_require_unique():
    #check if prop is in uniquekeys after taking out system properties
    #submitter ids need to be unique (both from parent or current node). Put other columns that need to be unique here
    if 'submitter_id' in prop_name:
        is_unique = True 

    return is_unique

def check_is_required(property,links,required):
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
    pass

def define_dtype(property):
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
            dtype = str 
        elif float in enum_dtype:
            dtype = float 
        elif int in enum_dtype:
            dtype = int
        else: #TODO: add other possibilities? raise error as in current validate fxn?
            dtype = object
    elif '$ref' in prop_vals and prop_name in links:
        dtype = str
    #TODO: in future may want to build this out but for now just skipping (eg id, create date)
    elif '$ref' in prop_vals: 
        return None
    else:
        raise Exception("No type in property...something is wrong with yaml file")
    return dtype

def check_enum(checks,property):
    if 'enum' in prop_vals:
        checks.append(pa.Check.isin(prop_vals['enum']))

def add_parent_prop_name(property,links,parent_prop_name='submitter_id'):
    #change prop name if its a parent node property -- right now just submitter_id
    #TODO: use $ref -- split on #/ -- and reference yaml file instead of hard coding
    if prop_name in links:
        prop_name = f"{prop_name}.{parent_prop_name}"
    return prop_name

def build_schema(properties,links,required):
    '''
    given a dictionary containing all the node jsons 
    (where the keys are yaml file names)
    and the desired node, build a pandera DataSchema object
    '''
    exclude = ['type']
    properties = [p for p in properties.keys()
                    if p not in system_properties + exclude]
    required = [p for p in required if p not in exclude]
    links = {link['name']:link for link in links}
    pa_columns = {}
    properties = type_json['properties']
    for prop_name,prop_vals in properties.items():
        #determine checks needed. May want to add other custom for non-enum etc
        column_args['checks'] = checks = []
        {property_name:pa.Column(**column_args)}
        pa_columns.update(build_column(prop_name,prop_vals,required,links))
    schema = pa.DataFrameSchema(pa_columns)
    return schema


