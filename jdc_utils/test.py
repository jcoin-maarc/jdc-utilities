from jdc_utils.submission import submit_protocols,submit_tsvs
import os


with open(protocol_info,'r') as f:
    hub_protocol_info = yaml.safe_load(f)


base_dir = 'c:\\Users\\kranz-michael\\projects\\rcg-bsd-gitlab\\jcoin-maarc\\hubs\\general'
submit_protocols(os.path.join(base_dir,'protocols.yaml'),os.path.join(base_dir,'credentials.json'))
submit_tsvs(os.path.join(base_dir,'protocols.yaml'),os.path.join(base_dir,'credentials.json'))


#%%
from transforms import map as map_jdc 
import pandas as pd

datafile = 'c:/Users/kranz-michael/Downloads/COPYJCOINSAEReportin_DATA_2021-09-30_1617.csv'
mapfile = 'c:/Users/kranz-michael/projects/rcg-bsd-gitlab/jcoin-maarc/hubs/general/sae_mapping.yaml'


df = pd.read_csv(datafile)
map_jdc(df,mapfile)
