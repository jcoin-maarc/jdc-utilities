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

def replace_ids(df, id_file, map_file, map_url=None, level=0, column=None):
    """Replace IDs in DataFrame and store mapping
    
    Index column containing old IDs may be either named or unnamed. With
    MultiIndex, level is used to identify the ID column to be replaced. If
    column is provided, this is used to identify a column or index level name
    (overrides level). Name of resulting ID column is taken from name in
    header of id_file.
    
    Note: A lot of work has gone into ensuring that this function will work
    with all possible cases of indexes/multi-indexes, both named and unnamed,
    and with/without the column argument (including with IDs not located in
    the index). No work has been done yet to examine the performance of the
    resulting function or whether it could be improved (2020-12-08).

    By default, the index is located as first index level.

    :param df: input data file (pd.DataFrame)
    :param id_file: list of generated ids (see the generate_ids function)
    :param map_file: csv file where id mappings are stored -- this will be generated if it doesn't exist
    :param map_url: the git bare repository where history is stored
    :param level: if in index, this indicates which level ids are located (default is first level (0))
    :param column: name of column or index where ids are stored for dataframe

    
    """
    
    # if not map_url:
    #     map_url = config['map_url'].get(str)
    
    index = True

    #check if the specified column name is within one of the index levels (can be a Multiindex)
    if column:
        if column in df.index.names:
            level = df.index.names.index(column)
        else:
            index = False
            try:
                colpos = df.columns.get_loc(column)
            except KeyError:
                raise Exception('Column {} not found in dataframe'.format(column))
    
    # get entire list of input datasets  ("old") ids

    ## if column name is in one of the indices, get these values and the old index name or use __id if no name
    if index:
        old_ids = df.index.get_level_values(level).to_series()
        old_name = (old_ids.name if old_ids.name else '__id')
        old_ids.name = old_name
    else:
        old_ids = (
            df[column]
            .reset_index(drop=True)
        )
        old_name = column


    # read in the file with list of ids generated from the generate id functions with a header in this file representing new name
    ids = pd.read_csv(id_file)
    new_name = ids.columns[0] 
    # using the git repo contenxt manager created in tools.py: given mapping file and remote url 
    # 
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
        
        ## find ids needed by merging the entire list of old ids with
        ## the already-mapped ids and finding the ids
        need_ids = (
            old_ids
            ## make sure its only the unique ids and in a dataframe
            .drop_duplicates(keep='first')
            .to_frame()
            .merge(
                mapped_ids, how='left', on=old_name, indicator=True,
                validate='one_to_one'
            )
            .pipe(lambda df: df.loc[df['_merge'] == 'left_only', [old_name]]) #filter ids not mapped and then select old id column
            .reset_index(drop=True)
        )
        
        # get unused ids from the generated id list 
        available_ids = (
            ids
            .merge(
                mapped_ids, how='left', on=new_name, indicator=True,
                validate='one_to_one'
            )
            .pipe(lambda df: df.loc[df['_merge'] == 'left_only', [new_name]]) #filter ids not mapped and then select old id column
            .reset_index(drop=True)
        )
        
        if len(need_ids) > len(available_ids):
            raise Exception('Too few new IDs')

        ## Required to avoid duplicating "__id" as both index level and column label
        need_ids.index.name = None

        ## map the available ids pulled from the id bank and save these new mappings to the end of the mapping file
        new_map = need_ids.merge(available_ids, how='left', left_index=True,
                                 right_index=True, validate='one_to_one')
        new_map.sort_values(by=old_name, inplace=True)
        new_map.to_csv(f, index=False, header=add_header,line_terminator="\n")
        
        ## now that we've added the new id mappings, re-read in the mapped ids file.
        f.seek(0)
        mapped_ids = pd.read_csv(f)
        
        idx_names = df.index.names
        
        if '__id' not in idx_names:
            df['__id'] = old_ids.tolist()
        # should this include an else statement for the reset index for easier readability?
        df.reset_index(drop=False, inplace=True)
        df = df.merge(mapped_ids, how='left', left_on='__id', right_on=old_name)
        
        # if old name is in the the index
        if index:
            df.iloc[:,level] = df[new_name]
            df.rename(columns={df.columns[level]:new_name}, inplace=True)
            idx = pd.MultiIndex.from_frame(df.iloc[:,:len(idx_names)])
            df.set_index(idx, drop=True, inplace=True, verify_integrity=False)
            idx_names = list(idx_names)
            idx_names[level] = new_name
            df.rename_axis(idx_names, axis=0, inplace=True)
            
        else:
            idx = pd.MultiIndex.from_frame(df.iloc[:,:len(idx_names)],
                                           names=idx_names)
            df.set_index(idx, drop=True, inplace=True, verify_integrity=False)
            df.iloc[:,colpos+len(idx_names)] = df[new_name]
            df.rename(columns={df.columns[colpos+len(idx_names)]:new_name},
                      inplace=True)
        return df
        #return df.iloc[:,range(len(idx_names),len(idx_names)+len(df.columns))]



def generate_submitter_ids(num_ids=10000,filepath='submitter_ids.txt'):
    """Generate submitter IDs for use in submitting data to JDC
    
    added this function from UKY repo
    """

    ids = IDList(ids=generate_ids(n=num_ids, offset=100000, prefix='J', check_digit=True, length=8))
    ids.ids = [id[:4] + '-' + id[4:] for id in ids.ids]
    ids.write_ids_to_file(filepath, column_name='submitter_id')