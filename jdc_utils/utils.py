"""Utilities supporting the jdc-utilities package"""

import pandas as pd
import yaml
import re
import sys
from collections import OrderedDict
from numpy.random import RandomState
#TODO: make variable names more explicit and build out documentation for notes in each function
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
    """Discrete marginal distribution"""
    
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
