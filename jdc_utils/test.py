
import yaml
import pandas as pd
from io import read_df
from transforms import run_transformfile
from submission import Node

config_path = "../examples/config.yaml"
with open(config_path,'r') as f:
    config = yaml.safe_load(f)
for config in configs:
    #df
    #df = pd.read_excel('../tmp/KY_staff_surveys_20210607_clear.xlsx')
    df = read_df(config['data_file_path'])
    #run_transform with transform_file
    run_transformfile(df,config['variable_transform_file_path'])
    #validate submissions with participant and demographic nodes
    tsvs = []
    for node_name in config['variable_categories']:
        node = Node(node_name)
        node.to_tsv(df)





