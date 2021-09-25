#%%
# from jdc_utils.utils import df_from_marginals
from jdc_utils.transforms import read_mapfile
import pandas as pd
from jdc_utils.utils import get_cell
import yaml 


# test = read_mapfile('data_mgmt/summaries/staff_marginal_mappings.yaml')

#%%
template = pd.read_excel(filepath, header=None, index_col=None, dtype='object')
template.fillna('',inplace=True)
template.columns = list('ABCDEFG')
template.index+=1
template.index = template.index.astype(str)

template['B'] = 'B' + template.index
template['F'] = 'F' + template.index

template.rename(columns={'A':'label','B':'n'},inplace=True)
with open(layout_path,'w') as file:
    json_records = template[['label','n']].to_dict(orient='records')
    layout = yaml.safe_dump(json_records,file)


test = read_mapfile(layout_path)