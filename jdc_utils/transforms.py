"""Data transformations"""

import pandas as pd
import yaml
from collections import OrderedDict



class TransformDF(pd.DataFrame):
    #init pd.DataFrame


    def read_transformfile(transformfile):
        with open(transformfile) as file:
            return yaml.safe_load(file)
    
    def run_transformfile(transformfile):
        ''' 
        loop through transformations and mappings as specified 
        in the yaml file

        then runs a given function name with a set of paramaters.
        Intended to transform the dataframe inplace.
        To provide compatability with native pandas fxns, 
        the inplace argument assumed to be a parameter. 

        If kwargs, then need to register a function 
        calling the dictionary as keyword args in TransformDf class.
        '''
        transform_mappings = OrderedDict(read_transformfile(transformfile))

        for fxn_name,params in transform_mappings.items():
            getattr(self,fxn_name)(**params,inplace=True)

        

    def to_quarter(self,date_name,quarter_name,inplace=True):
        ''' 
        adds a quarter column by converting a date-like column
        into a quarter
        '''
        var = pd.to_datetime(self[date_name])
        self[quarter_name] = pd.PeriodIndex(var, freq='Q')

    def collapse_checkall(self, collapsed_name,columns, checked='Checked',
                        multi_checked='Multiple checked',
                        none_checked='None checked', fillna=False, labels=None,inplace=True):
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
        
        df[collapsed_name] =  var



