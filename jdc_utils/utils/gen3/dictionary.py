"""Utilities for working with Gen3 Commons data dictionaries and corresponding submissions"""
import json 
import jsonschema
import requests


# submission validation utilities for gen3 sheepdog validation 
def get_dictionary(endpoint,node_type='_all'):
    ''' 
    get json dictionary with all references (eg if $ref then API has these properties of this ref)
    
    _all gets entire schema while node names get only that node type
    ''' 
    api_url = f"{endpoint}/api/v0/submission/_dictionary/{node_type}"
    output = requests.get(api_url).text
    dictionary = json.loads(output)
    return dictionary

class Node:
    '''
    object represents a gen3 node with gen3 specific properties 
    and standard json schema properties for:
        1. validating records
        2. writing a validated set of records to specified file format
        3. node property look up
    '''
    def __init__(self,endpoint,type):
        self.type = type
        dictionary = get_dictionary(endpoint,type) #note - full dictionary will be needed for node refs in future versions
        
        self.system_properties = dictionary['systemProperties']
        self.links = dictionary['links']
        self.required = dictionary['required']
        self.unique_keys = dictionary['uniqueKeys']
        #get just the link names
        self.link_names = [x.value for x in parse("$..name").find(self.links)]

        self.schema = pa.DataFrameSchema.from_schema(dictionary)


        return report

    def write(self,df,return_type="records"):
        ''' 
        validates and outputs to specified format 
        (ie file, json array of records, or printed tsv string)
        '''
        data = df.copy()
        #make submitter_id index if not already index\
        index_name = "submitter_id"

        if data.index.name!=index_name and index_name in data.columns:
            data.set_index(index_name,inplace=True)
        
        validated_data = self.schema.validate(df)

        if return_type=='file':\
            return validated_data.to_csv(self.type+".tsv",sep='\t')

        if return_type=="records":
            return validated_data.to_dict(orient="records")
        elif return_type=='tsv':
            return validated_data.to_csv(sep="\t")
        else:
            raise Exception("Need to specify one of the return types: file,records,or tsv")
