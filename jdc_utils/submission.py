"""Generate TSV files for submission to JDC"""

from urllib.request import urlopen
import json
import sys
from jdc_utils.dictionary import build_schema,get_dictionary
from jdc_utils.transforms import map as map_jdc

#manifest used for production deployment
MANIFEST_URL = 'https://raw.githubusercontent.com/uc-cdis/cdis-manifest/master/jcoin.datacommons.io/manifest.json'


class Node:
    
    def __init__(self, type):
        
        self.type = type
        dictionary = get_dictionary(MANIFEST_URL) #note - full dictionary will be needed for node refs in future versions
        
        self.system_properties = dictionary[f'{type}.yaml']['systemProperties']
        self.properties = dictionary[f'{type}.yaml']['properties']
        self.links = dictionary[f'{type}.yaml']['links']
        self.required = dictionary[f'{type}.yaml']['required']
        self.unique_keys = dictionary[f'{type}.yaml']['uniqueKeys']

        self.schema = build_schema(
            self.properties,
            self.links,
            self.required,
            self.unique_keys,
            self.system_properties
        )
    
    def map_df(self,df,mapfile,to_tsv=False):
        data = df.copy()
        map_jdc(data, mapfile)
        cols = self.schema.columns
        data = data.loc[:,data.columns.isin(cols)]
        self.data = data
        self.validated_data = self.schema.validate(self.data)
        if to_tsv:
            self.validated_data.to_csv(path_or_buf, sep='\t')