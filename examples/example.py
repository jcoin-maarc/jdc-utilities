
import yaml
import pandas as pd
from jdc_utils.transforms import run_transformfile,read_df
from jdc_utils.submission import Node
from pathlib import Path
from jdc_utils.utils import generate_submitter_ids
## make ids
#test = Path("../data_mgmt/id_store")
#generate_submitter_ids(filepath=f"{test.as_posix()}/submitter_ids.txt")

config_path = Path(r"C:\Users\kranz-michael\projects\rcg-bsd-gitlab\jcoin-maarc\jdc-utilities\examples\example_config.yaml")
with open(config_path,'r') as f:
    configs = yaml.safe_load(f)

#%%
for config in configs['datafiles']:
    print(config)
    df = read_df(config['data_file_path'])
    run_transformfile(df,transformfile=config['variable_transform_file_path'])
    #validate submissions with participant and demographic nodes
    for node_name in config['variable_categories']:
        data = df.copy()
        node = Node(node_name)

        #add the upstream submitter_id linked to one_to_one node (eg demographic)
        #TODO: add link submitter ids programatically to Node.to_tsv for one_to_one links?
        if node_name!='participant':
            #convention is to have reference of upstream nodes to have plural 
            #e.g., participant node referenced as participants
            data['participants.submitter_id'] = data['submitter_id'].copy()

        #write to file
        tsv_file = f"{config['hub_name']}_{config['id']}_{node_name}.tsv"
        print(tsv_file)
        node.to_tsv(data,file_dir=config['outputted_tsv_file_dir'],file_name=tsv_file)


# from dataforge.ids import replace_ids

# path = lambda x: Path(x).as_posix()
# replace_ids_dict = dict(
#   id_file=path(r"C:\Users\kranz-michael\projects\rcg-bsd-gitlab\jcoin-maarc\jdc-utilities\data_mgmt\id_store\submitter_ids.txt"),
#   map_file= path(r"C:\Users\kranz-michael\projects\rcg-bsd-gitlab\jcoin-maarc\jdc-utilities\data_mgmt\id_store\local_to_jdc_ids.csv"),
#   map_url=path(r"C:\Users\kranz-michael\projects\rcg-bsd-gitlab\jcoin-maarc\hubs\general\repos\test-secrets.git")
# )
# df = read_df(configs['datafiles'][0]['data_file_path'])
# replace_ids(df,**replace_ids_dict)