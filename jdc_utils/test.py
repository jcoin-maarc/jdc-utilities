
import pandas as pd
from transforms import run_transformfile
from submission import Node
#df
df = pd.read_excel('../tmp/KY_staff_surveys_20210607_clear.xlsx')
#run_transform with transform_file
transform_file = '../examples/transforms.yaml'
run_transformfile(df,transform_file)
#validate submissions with participant and demographic nodes
tsvs = []
for node_name in ['participant','demographic']:
    node = Node(node_name)
    node.to_tsv(df)