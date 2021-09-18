# Generate quarterly report submission for staff data (from Excel file)
from pathlib import Path
from jdc_utils.submission import Node
import pandas as pd
import os


FILEPATH = 'tmp/KY_staff_surveys_20210607_clear.xlsx'
df = pd.read_excel(FILEPATH, index_col=0, header=0, skiprows=[1])

participant = Node('participant')
participant.map_df(df,'data_mgmt/staff_map.yaml')
participant.add_submitter_ids(ids=participant.unvalidated_data.index)
participant.add_submitter_ids(parent_node='protocol',ids='main')
participant.add_quarter(from_column='date_recruited')
participant.add_role_in_project('Staff')
participant.validate_df()
participant.to_tsv(file_dir='tmp/gen3/staff',file_name='participant.tsv')

# Write data for demographic node
demographic = Node('demographic')
demographic.to_tsv(data, 'tmp/gen3/staff/demographic.tsv', add_suffix=True,submitter_id=data.index)