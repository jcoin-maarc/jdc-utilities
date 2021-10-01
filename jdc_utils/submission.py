"""Generate TSV files for submission to JDC"""

from urllib.request import urlopen
import json
import sys
from jdc_utils.dictionary import NodeDictionary
from jdc_utils.transforms import map as map_jdc
from jdc_utils.transforms import to_quarter
from pathlib import Path
import os
import yaml

from gen3.auth import Gen3Auth
from gen3.submission import Gen3Submission
import yaml
import requests
import sys

#manifest used for production deployment 
MANIFEST_URL = 'https://raw.githubusercontent.com/uc-cdis/cdis-manifest/master/jcoin.datacommons.io/manifest.json'
#Gen3 API constant variables
ENDPOINT = 'https://jcoin.datacommons.io/'
PROGRAM = 'JCOIN'

class NodeSubmission:
    
    def __init__(self, type,manifest_url=MANIFEST_URL):
        self.node_dictionary = NodeDictionary(manifest_url=MANIFEST_URL,type=type)

    def map_df(self,df,mapfile=None):
        data = df.copy()
        if mapfile:
            map_jdc(data, mapfile)
        if 'type' not in data.columns:
            data.insert(0,'type',self.type)
        self.unvalidated_data = data
        return self

    def add_submitter_ids(self,ids,parent_node=None,is_index=True):
        #TODO: replace_id function from dataforge here?
        if parent_node:
            col_name = f"{parent_node}.submitter_id"
        else:
            col_name = "submitter_id"
        
        if is_index and not parent_node: #parent nodes cant be index
            self.unvalidated_data.index = ids
            self.unvalidated_data.index.name = col_name
        else:
            self.unvalidated_data[col_name] = ids
        return self

    # def add_quarter(self,from_column='date_recruited'):
    #     #TODO: integrate as a tranform into schema?
    #     self.unvalidated_data['quarter_recruited'] = to_quarter(
    #         self.unvalidated_data[from_column]
    #         ).fillna('Not reported').astype(str)
    #     return self

    # def add_role_in_project(self,role):
    #     self.unvalidated_data['role_in_project'] = role
    #     return self

    def validate_df(self,return_validated_df=False):
        self.validated_data = (
            self
            .node_dictionary
            .schema
            .validate(self.unvalidated_data)
        )
        if return_validated_df:
            return self.validated_data
        else:
            return self

    def to_tsv(self,file_dir,file_name,index=True,return_self=True):
        Path(file_dir).mkdir(parents=True, exist_ok=True)
        self.validated_data.to_csv(os.path.join(file_dir,file_name),index=index)

        if return_self:
            return self
        else:
            pass

    # def to_gen3(self,credentials_file_path,project,program=PROGRAM,endpoint=ENDPOINT):
    #     '''
    #     uploads the validated data to the gen3 commons

    #     WIP and has not been tested/used yet.
        
    #     TODO: may want to make gen3 properties a part of the init statement
    #     and/or as part of the NodeDictionary class. This way, more fxns can
    #     be added that queries records that have been uploaded 
    #     (or as a programmatic way to download records).
    #     '''

    #     #make auth object auth with credentials_file_path
    #     json_of_validated_df = df_to_json_records(self.validated_data)
    #     self.submission_request_feedback = submit_to_gen3(
    #         endpoint=endpoint,
    #         program=PROGRAM,
    #         project=project,
    #         credentials_file_path=credentials_file_path,
    #         json_of_records=json_of_validated_df)
    #     return self


def df_to_json_records(df):
    '''
    converts
    a dataframe into a list of json records
    '''
    json_str = df.to_json(orient='records')
    return json.loads(json_str)
    
def submit_protocols(protocol_info='protocols.yaml',credentials_file_name = "credentials.json",endpoint=ENDPOINT):
    '''
    submit protocol(s) for several projects

    Takes a file called protocols.yaml containing info that changes across protocols, formats a dict, and submits.
    Will exit with detailed error if submission not successful

    TODO: add more individaul protocol levels to protocols.yaml and make protocol_json loop through programmaticallly
    rather than hardocding.
    '''
    with open(protocol_info,'r') as f:
        hub_protocol_info = yaml.safe_load(f)
    auth = Gen3Auth(refresh_file=credentials_file_name)
    sub = Gen3Submission(auth)

    for project,properties in hub_protocol_info.items():
        protocol_cluster = properties['protocol']
        protocol_json = [
            {
                'submitter_id':'main',
                'project_id':PROGRAM+'-'+project,
                'type':'protocol',
                #'research_question': research_question,
                'protocol':protocol_cluster,
                'projects':{'code':project}
            }
        ]
        api_url = "{}/api/v0/submission/{}/{}".format(ENDPOINT,PROGRAM,project)
        output = requests.put(api_url, auth=auth, json=protocol_json)


def submit_tsvs(protocol_info='protocols.yaml',credentials_file_name="credentials.json"):

    with open(protocol_info,'r') as f:
        hub_protocol_info = yaml.safe_load(f)
    auth = Gen3Auth(refresh_file=credentials_file_name)
    sub = Gen3Submission(auth)

    for project,properties in hub_protocol_info.items():
        tsv_paths = properties['tsv_paths']
        for tsv in tsv_paths:
            api_url = "{}/api/v0/submission/{}/{}".format(ENDPOINT,PROGRAM,project)
            output = requests.put(api_url, auth=auth, tsv=tsv)
    

