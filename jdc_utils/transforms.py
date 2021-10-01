"""Data transformations"""

import pandas as pd
import yaml

def read_mapfile(mapfile):
    with open(mapfile) as file:
        return yaml.safe_load(file)

def map(df, mapfile):
    """Rename vars and/or replace values"""
    
    map = read_mapfile(mapfile)
    column_props = map['columns']
    for var,props in column_props.items():

        if 'add_constant' in props:
            df[var] = props['add_constant']

        if 'to_submitter_id' in props:
            df[props['name']] = df[var]

        if 'to_quarter' in props:
            if props['to_quarter']:
                df[props['name']] = to_quarter(df[var]).fillna('Not reported').astype(str)

        if 'values' in props:
            df[var].replace(props['values'], inplace=True)

        if 'name' in props:
            df.rename(columns={var: props['name']}, inplace=True)

    #if there are checkboxes, then run through them all
    if 'checkboxes' in map:
        checkbox_props = map['checkboxes']
        checkbox_columns = {
            var:props['checkbox'] 
            for var,props in column_props.items() 
            if 'checkbox' in props
        }

        for group,props in checkbox_props.items():
            columns = [
                var for var,props in checkbox_columns.items() 
                if props['group']==group
            ]
            labels = [
                props['label'] for var,props in checkbox_columns.items() 
                if props['group']==group                
            ]
            
            df[group] = collapse_checkall(
                df, 
                columns=columns, 
                labels=labels,
                checked=props['checked'],
                multi_checked=props['multi_checked'],
                none_checked=props['none_checked'], 
                fillna=props['fillna']
            )


        
def add_submitter_ids(df,ids,parent_node=None,is_index=True):
    #TODO: replace_id function from dataforge here?
    if parent_node:
        col_name = f"{parent_node}.submitter_id"
    else:
        col_name = "submitter_id"
    
    if is_index and not parent_node: #parent nodes cant be index
        df.index = ids
        df.index.name = col_name
    else:
        df[col_name] = ids

def to_quarter(datevar):
    
    var = pd.to_datetime(datevar)
    return pd.PeriodIndex(var, freq='Q')

def collapse_checkall(df, columns, checked='Checked',
                      multi_checked='Multiple checked',
                      none_checked='None checked', fillna=False, labels=None):
    """Collapse multiple check-all-that-apply fields into one"""
    
    bcols = df[columns]==checked
    var = bcols.idxmax(axis=1).where(bcols.sum(axis=1)==1, multi_checked).\
                               where(bcols.sum(axis=1)>0, none_checked)
    
    var = var.where((df[columns].notnull().sum(axis=1)==len(columns)) |
                    (var==multi_checked), None)
    
    if labels:
        var.replace(dict(zip(columns,labels)), inplace=True)
    
    if fillna:
        var.fillna(fillna, inplace=True)
    
    return var
