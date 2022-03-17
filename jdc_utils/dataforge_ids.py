"""Utilities for generating and manipulating IDs

Note: The implementation of the Verhoeff check-digit algorithm was written by
Michael Johnson, and is based on javascript code located at
http://www.augustana.ab.ca/~mohrj/algorithms/checkdigit.html.
"""

import csv, re
#from dataforge import config
#from dataforge.tools import versioned_file_resource
from jdc_utils.dataforge_tools import versioned_file_resource
import pandas as pd

# Implementation of Verhoeff check-digit algorithm
# Respresent the group d5, i.e. symmetries of regular pentagon.
d5=[[0,1,2,3,4,5,6,7,8,9],
    [1,2,3,4,0,6,7,8,9,5],
    [2,3,4,0,1,7,8,9,5,6],
    [3,4,0,1,2,8,9,5,6,7],
    [4,0,1,2,3,9,5,6,7,8],
    [5,9,8,7,6,0,4,3,2,1],
    [6,5,9,8,7,1,0,4,3,2],
    [7,6,5,9,8,2,1,0,4,3],
    [8,7,6,5,9,3,2,1,0,4],
    [9,8,7,6,5,4,3,2,1,0]]

# Permutation table -- applies permutation to digit based on its position in
# the number.
def construct_perm():
    perm=[[0,1,2,3,4,5,6,7,8,9]]
    perm.append([1,5,7,6,2,8,3,0,9,4])
    for i in range(2,8):
        perm.append([None]*10)
        for j in range(10):
            perm[i][j]=perm[i-1][perm[1][j]]
    return perm

perm=construct_perm()

# Inverses: inv[i]=j means d5[i][j]=0
inv=[0,4,3,2,1,5,6,7,8,9]
for i in range(10):
    j=inv[i]
    assert d5[i][j]==0

def verhoeff_alg(val):
    """val should be a sequence of items that can be cast as integer"""
    val_list=[int(i) for i in reversed(tuple(val))]
    c=0
    for pos,i in enumerate(val_list):
        c = d5[c][perm[pos%8][i]]
    return c

def check(val):
    c=verhoeff_alg(val)
    if c==0:
        return True
    else:
        return False

def compute(val):
    val=list(val)
    val.append(0)
    c=verhoeff_alg(val)
    return str(inv[c])


def convert_to_int(c):
    """Accepts string of length one containing character from class [a-zA-Z0-9]
    and returns character in class [0-9]; integers are returned unchanged,
    while alphabetic characters are converted into a number 0-25, modulo 10
    """
    
    e = re.compile('^[a-zA-Z0-9]$')
    try:
        if not e.match(c):
            raise ValueError("convert_to_int() expected character in class " \
                             "[a-zA-Z0-9], but found '%s'" % c)
    except TypeError:
        raise TypeError("convert_to_int() expected string, but found %s" % c)
    
    if ord('0') <= ord(c) <= ord('9'):
        return c
    elif ord('a') <= ord(c) <= ord('z'):
        return str((ord(c) - ord('a'))%10)
    elif ord('A') <= ord(c) <= ord('Z'):
        return str((ord(c) - ord('A'))%10) 

def add_cd(id):
    """Adds check digit to end of ID composed solely of alphanumeric characters
    (including both lower and upper case)"""
    
    try:
        converted_id = ''.join([convert_to_int(c) for c in id])
    except TypeError:
        raise TypeError("add_cd() expected string, but found %s" % id)
    return id + compute(converted_id)

def generate_ids(n=0, prefix='', offset=0, length=None, check_digit=False):
    """Generate list of IDs"""
    
    l = len(prefix + str(n + offset))
    if check_digit:
        l = l + 1
    if length and length > l:
        l = length
    
    if check_digit:
        f = '%%0%sd' % (l - len(prefix) - 1)        
    else:
        f = '%%0%sd' % (l - len(prefix))
    ids = [prefix + f % i for i in range(offset + 1, n + offset + 1)]
    
    if check_digit:
        return [add_cd(id) for id in ids]
    else:
        return ids

class IDList:
    """A class for storing and manipulating a list of IDs"""
    
    def __init__(self, ids=None):
        if ids:
            self.ids = ids
        else:
            self.ids = []
    
    def add_to_ids(self, additional_ids):
        self.ids.extend(additional_ids)
    
    def write_ids_to_file(self, filename, column_name=None, mode=None):
        
        if not mode:
            mode = 'w'
        
        f = open(filename, modenewline='')
        id_writer = csv.writer(f)
        if column_name:
            id_writer.writerow([column_name])
        for id in self.ids:
            id_writer.writerow([id])
        
        f.close()

def replace_ids(df,id_file, map_file, map_url=None, column=None,inplace=False,drop_old_name=True):
    """Replace IDs in DataFrame and store mapping

    TODO: add capability to assign multiple columns/Multiindex --- simplified for JDC use case but can easily be extended 
    """
    ## get the "old" (or local) ids and assign that as the old name
    if column:
        old_name = column
        if column in df.columns:
            old_ids = df[column].copy()
        elif column in df.index.names:
            old_ids = df.index.get_level_values(column).to_series()
        else:
            raise Exception(f'Column {column} not found in dataframe')
    else:
        exception_message = (
            "'column' not specified."
            "If you are replacing on the DataFrame index,"
            "please first name index and pass this name to 'column' parameter"
        )
        raise Exception(exception_message)
    old_ids.reset_index(drop=True,inplace=True) #make index just integer locations


    # get ids generated from the id bank
    all_ids = pd.read_csv(id_file)
    new_name = all_ids.columns[0] 

    # using the git repo contenxt manager created in tools.py: given mapping file and remote url 
    with versioned_file_resource(map_file=map_file, remote_url=map_url, mode='a+') as f:
        f.seek(0)
        try:
            ## if able to read in data, so will append file further down stream in this code (and wont want headers)
            mapped_ids = pd.read_csv(f)
            add_header = False
        ## if no data in file that was created or opened by context manager
        except pd.errors.EmptyDataError:
            #initiate a mapping df to add headers
            mapped_ids = pd.DataFrame(columns = [old_name, new_name])
            # indicates headers will be added to new data as file is empty
            add_header = True
        

        #get the ids needed to map and the available/unused ids from the generated list to map to these unmapped ids
        def _filter_out_mapped_ids(series):
             series.loc[~series.isin(mapped_ids[series.name])]
             return series

        need_ids = (
            old_ids
            .drop_duplicates(keep='first') ## make sure its only the unique ids and in a dataframe
            .pipe(_filter_out_mapped_ids)
        )
        
        available_ids = (
            all_ids[new_name]
            .pipe(_filter_out_mapped_ids)
            
        )
        
        if len(need_ids) > len(available_ids):
            raise Exception('Too few new IDs')

        ## assign available ids pulled from the generated id bank and save these new mappings to the end of the mapping file
        new_map = pd.concat([need_ids,available_ids],axis=1,join='inner')
        new_map.sort_values(by=old_name, inplace=True)
        new_map.to_csv(f, index=False, header=add_header,line_terminator="\n") #added line terminator so no extra lines on windows
        
        ## now that we've added the new id mappings, re-read in the newly mapped ids file.
        f.seek(0)
        mapped_ids = pd.read_csv(f)
        df_with_new_ids = df.merge(mapped_ids, how='left', on=old_name)

        if inplace:
            df[new_name] = df_with_new_ids[new_name].to_list()#add new ids inplace to original input df
            if drop_old_name:
                df.drop(columns=old_name,inplace=True)
        else:
            if drop_old_name:
                df_with_new_ids.drop(columns=old_name,inplace=True)
            return df_with_new_ids



def generate_submitter_ids(num_ids=10000,filepath='submitter_ids.txt'):
    """Generate submitter IDs for use in submitting data to JDC
    
    added this function from UKY repo
    """
    ids = IDList(ids=generate_ids(n=num_ids, offset=100000, prefix='J', check_digit=True, length=8))
    ids.ids = [id[:4] + '-' + id[4:] for id in ids.ids]
    ids.write_ids_to_file(filepath, column_name='submitter_id')