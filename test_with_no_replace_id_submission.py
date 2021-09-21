# Generate quarterly report submission for staff data (from Excel file)
from jdc_utils.submission import NodeSubmission
import pandas as pd


FILEPATH = 'tmp/KY_staff_surveys_20210607_clear.xlsx'
df = pd.read_excel(FILEPATH, index_col=0, header=0, skiprows=[1])
participant = NodeSubmission('participant')
participant.map_df(df,'data_mgmt/staff_map.yaml')
(
    participant
    .add_submitter_ids(ids=[str(i) for i in range(len(df))])
    .add_submitter_ids(parent_node='protocols',ids='main')
    .add_quarter(from_column='date_recruited')
    .add_role_in_project('Staff')
)
participant.validate_df()
#
participant.to_tsv(file_dir='tmp/gen3/staff',file_name='participant.tsv')

demographic = NodeSubmission('demographic')
demographic.map_df(df,'data_mgmt/staff_map.yaml')
(
    demographic
    .add_submitter_ids(ids=[str(i)+'D' for i in range(len(df))])
    .add_submitter_ids(
        parent_node='participants',
        ids=participant.validated_data.index
    )
)
demographic.validate_df()
demographic.to_tsv(file_dir='tmp/gen3/staff',file_name='demographic.tsv')