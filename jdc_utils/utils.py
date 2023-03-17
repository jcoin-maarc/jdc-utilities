"""Utilities supporting the jdc-utilities package"""

import pandas as pd
import yaml
import re
import sys
from collections import OrderedDict
from numpy.random import RandomState
import shutil
import random
from pathlib import Path
from zipfile import ZipFile


#general utilities
def copy_file(file_path, target_path):
    with open(file_path, "rb") as source:
        with open(target_path, "wb") as target:
            shutil.copyfileobj(source, target)


#utilities for collecting aggregated data from templates
def get_cell(df, address):
    """Get contents of cell in df based on Excel address (e.g., A1)"""
    
    pattern = re.compile('([A-Z]+)([0-9]+)')
    m = pattern.match(address.upper())
    try:
        col = 0
        for letter in m.group(1):
            col = col*26 + (ord(letter) - ord('A'))
        row = int(m.group(2)) - 1
        contents = df.iat[row, col]
    except AttributeError:
        sys.exit(f'Cell address "{address}" invalid')
    except IndexError:
        sys.exit(f'Cell "{address}" not found')
    
    return contents

class Marginal:
    """Discrete marginal distribution
    
    df: dataframe of values from an excel file
    m: "marginals" --> list of dict objects containing
        the variable name ("variable") and 
        list of values (the excel location "n" and "label" or  the name of variable level)

    n: the total number within marginal used as a check    
    
    """
    
    def __init__(self, df, m, n):
        
        self.name = m['variable']
        self.values = OrderedDict()
        sum = 0
        for v in m['values']:
            try:
                self.values[v['label']] = int(get_cell(df, v['n']))
                sum += self.values[v['label']]
            except ValueError:
                pass
        
        try:
            self.n = int(n)
            if sum>n:
                sys.exit(f'Sum of marginals for {self.name} exeeds total {self.n}')
            else:
                self.sum = sum
        except ValueError:
            sys.exit(f'Invalid value for n: {n}')
    
    def series(self, fillna=False, order=None, random_state=None):
        """Return Series corresponding to marginal"""
        
        slist = []
        for k, v in self.values.items():
            slist.append(pd.Series([k]*v))
        if self.sum < self.n:
            slist.append(pd.Series([None]*(self.n - self.sum)))
        series = pd.Series(name=self.name, dtype='object').append(slist,
                                                                  ignore_index=True)
        
        if fillna:
            series.fillna(fillna, inplace=True)
        
        if order == 'random':
            series = series.sample(frac=1, random_state=random_state).\
                            reset_index(drop=True)
        
        return series

def read_marginals(filepath, layout_path):
    """Read marginals from Excel file according to layout"""
    
    df = pd.read_excel(filepath, header=None, index_col=None, dtype='object')
    with open(layout_path) as file:
        layout = yaml.safe_load(file.read())
    
    n = get_cell(df, layout['n'])
    marginals = []
    for m in layout['marginals']:
        marginals.append(Marginal(df, m, n))
    
    return marginals

def df_from_marginals(filepath, layout_path, fillna=False, order='random',
                      seed=1234567890):
    """Generate data frame from list of marginals"""
    
    prng = RandomState(seed)
    marginals = read_marginals(filepath, layout_path)
    
    cols = []
    for m in marginals:
        cols.append(m.series(fillna=fillna, order=order, random_state=prng))
    
    df = pd.concat(cols, axis=1)
    return df




def split_marginals_and_total(df:pd.DataFrame) -> pd.DataFrame:
    """split into total and marginals

    Args:
        df (pd.DataFrame): _description_

    Returns:
        tuple[pd.DataFrame,pd.DataFrame]: _description_
    """
    totals = df.loc[df.index=='Total']
    marginals = df.loc[df.index!='Total']
    totals_diff = totals - marginals.sum()
    totals_exceed = totals_diff<0
    # check totals
    exit = False
    if totals_exceed.sum().sum()<0:
        mess = (
            f'For {sheet_name}:\n'
            'the sum of marginals exceed the reported total number\n'
        )
        exit = True 

    if exit:
        sys.exit(mess)

    # add not reported (happens if cell size restrictions)
    
    try:
        not_reported = marginals.loc['Not reported',:]
        not_reported+=totals_diff.values.flatten()
    except KeyError:
        marginals.loc['Not reported',:] = totals_diff.values.flatten()

    return marginals


# create individual level data

# multiply category by count
def create_marginal_sample(marginals:pd.DataFrame,column_name:str='column',category_name:str='category') -> pd.DataFrame:
    """Create the marginal sample (or individual level data)
    by multiplying the category names by the total count for that
    category


    Args:
        marginal (pd.DataFrame): _description_
        column_name (str): _description_
        category_name (str): _description_

    Returns:
        pd.DataFrame: _description_
    """
    sample = []
    for col,marginal in marginals.iteritems():
        for category,total in marginal.iteritems():
            if pd.notna(total):
                #shuffle within each group (ie col or column -- ie type of participant)
                col_sample = int(total)*[{column_name:col,category_name:category}]
                random.shuffle(col_sample)
                sample.extend(col_sample)
    
    return pd.DataFrame(sample)

def _create_sample(xls_path,sheet_name):
    sample_df = (
        pd.read_excel(xls_path,sheet_name=sheet_name)
        .pipe(
            lambda df: df.set_index(df.columns[0])
        )
        .pipe(
            split_marginals_and_total
        )
        .pipe(
            create_marginal_sample
        )
        .pipe(
            lambda df: df.set_index(df.columns[0],append=True)

        )
    )
    return sample_df
def create_gen3_submission(xls_path:str,category_name_mappings:dict) -> pd.DataFrame:
    dfs = [
    _create_sample(xls_path, sheet_name)\
        .rename(columns={'category':gen3_name})\
    for sheet_name,gen3_name in category_name_mappings.items()]
    
    return pd.concat(dfs,axis=1)



def _write_to_zip(input_path,zip_object):
    '''
    recursively writes file contents
    of a directory
    to a zip object (in write mode)

    TODO: write directories (so can write empty dirs) without using mkdir
    '''       
    input_path = Path(input_path).resolve()
    input_contents = input_path.glob("*")
    
    for path in input_contents:
        #relative directory to write dir/file within zip
        relative_path = path.relative_to(input_path)
        if path.is_file():
            zip_object.write(path,relative_path)
        else:
            #zip_object.mkdir(relative_dir) #ZipFile.mkdir only supported starting in python 3.11
            _write_to_zip(path,zip_object)

def zip_package(pkg_path,zip_path):
    ''' 
    takes a valid package and outputs 
    to a zipped file.

    package-name --> package-name.zip
    '''
    assert Path(pkg_path).is_dir()
    assert Path(zip_path).is_dir()

    pkg_path = Path(pkg_path).resolve()
    outzip_path = (Path(zip_path).resolve()/
        pkg_path.with_suffix('.zip').name)

    with ZipFile(outzip_path,'w') as pkg_zip:
        _write_to_zip(pkg_path,pkg_zip)