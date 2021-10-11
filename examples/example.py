
import yaml
import pandas as pd
from jdc_utils.transforms import run_transformfile,read_df
from jdc_utils.submission import Node

config_path = "example_config.yaml"
with open(config_path,'r') as f:
    configs = yaml.safe_load(f)
for config in configs['datafiles']:
    #df
    #df = pd.read_excel('../tmp/KY_staff_surveys_20210607_clear.xlsx')
    df = read_df(config['data_file_path'])
    #run_transform with transform_file
    run_transformfile(df,config['variable_transform_file_path'])
    #validate submissions with participant and demographic nodes
    tsvs = []
    for node_name in config['variable_categories']:
        data = df.copy()
        tsv_file_dir = config['outputted_tsv_file_dir']
        tsv_file = f"{config['hub_name']}_{config['id']}_{node_name}.tsv"
        node = Node(node_name)

        #need to coerce submitter_id to str to add suffixes in link sub ids below
        data['submitter_id'] = data['submitter_id'].astype(str)

        #add the upstream submitter_id linked to one_to_one node (eg demographic)
        #TODO: add link submitter ids programatically to Node.to_tsv for one_to_one links?
        
        if node_name!='participant':
            #convention is to have reference of upstream nodes to have plural 
            #e.g., participant node referenced as participants
            data['participants.submitter_id'] = data['submitter_id'].copy() 
            data['submitter_id'] = data['submitter_id'] + "_" + node_name[:3]
        node.to_tsv(data,file_dir=tsv_file_dir,file_name=tsv_file)

