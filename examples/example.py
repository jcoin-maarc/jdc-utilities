#%%
import yaml
import pandas as pd
from jdc_utils.transforms import run_transformfile,read_df
from jdc_utils.submission import Node
from jdc_utils.dataforge_ids import replace_ids
from pathlib import Path


config_path = Path("example_config.yaml")
#Gen3 API constant variables
ENDPOINT = 'https://jcoin.datacommons.io/'
PROGRAM = 'JCOIN'

with open(config_path,'r') as f:
    configs = yaml.safe_load(f)

replace_id_params_all_files = configs.get('replace_ids',None)

for config in configs['datafiles']:
    print(config)

    transform_file = config.get('variable_transform_file_path',None)
    variable_categories = config.get('variable_categories',None)
    replace_id_params_this_file = config.get('replace_ids',None)
    if replace_id_params:
        df = replace_ids(read_df(config['data_file_path']),**replace_id_params_all_files)
    elif replace_id_params_this_file:
        df = replace_ids(read_df(config['data_file_path']),**replace_id_params_this_file)
    else:
        df = read_df(config['data_file_path'])

    
    if transform_file:
        run_transformfile(df,transformfile=config['variable_transform_file_path'])

    if variable_categories:
        #validate submissions with participant and demographic nodes
        for node_name in variable_categories:
            data = df.copy()
            node = Node(ENDPOINT,node_name)
            #write to file
            tsv_file = f"{config['hub_name']}_{config['id']}_{node_name}.tsv"
            node.to_tsv(data,file_dir=config['outputted_tsv_file_dir'],file_name=tsv_file)