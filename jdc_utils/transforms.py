"""Data transformations"""

import pandas as pd
import yaml

def read_mapfile(mapfile):
    with open(mapfile) as file:
        return yaml.safe_load(file)

def map(df, mappings):
    """Rename vars and/or replace values"""
    for var in mappings:
        if var in df:
            if 'values' in mappings[var]:
                df[var].replace(mappings[var]['values'], inplace=True)

            if 'name' in mappings[var]:
                df.rename(columns={var: mappings[var]['name']}, inplace=True)

def to_quarter(datevar):
    
    var = pd.to_datetime(datevar)
    return pd.PeriodIndex(var, freq='Q')

def get_mappings(mappings,substr,mapping_property='name'):
    '''
    get a list of remapped names 
    based on a substring pattern of unmapped names

    returns a list of mapped names

    TODO: specifying whether name or value mapping list etc
    '''
    remapped_name_list = [
        name[mapping_property]
        for key,name in mappings.items()
        if substr in key
    ]
    return remapped_name_list

def combine_checkboxes(
    df,
    name_if_more_than_one_checked,
    value_of_checked="Checked",
    value_of_none_checked='Not reported'
    ):
    num_checked = (df==value_of_checked).sum()
    if num_checked>1:
        race_enum = name_if_more_than_one_checked
    elif num_checked==0:
        race_enum = value_of_none_checked
    else:
        race_enum = df.loc[df==value_of_checked].index[0]

    return race_enum