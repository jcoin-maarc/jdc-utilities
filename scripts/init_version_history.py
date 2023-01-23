"""
This script puts the current mappings under version
control and stores each type of mapping as a separate file
rather than in one file. 

The previous workflow did not require version control so this script
initiates the version control (ie history) component, which is desirable
because it allows for the possibility of "rolling back" additions if mappings
were added in error; provides a safety net if files become corrupt;
makes it easier to collaborate as the history can be stored in a shared space.

After you have verified the individual files have been created and are not versioned
(ie there are files with the path: tmp/git/<name>/<name>.csv)
""" 



from jdc_utils.transforms.deidentify import init_version_history
from pathlib import Path
import pandas as pd
import click
from dataforge.tools import versioned_file_resource
import os

map_file_help = """ 
File with mappings for 
(1) jdc_person_ids and 
(2) days for shifting dates assigned to each id.
"""
id_column_help = """ 
The local id (not the name 'jdc_person_id' but the originally assigned person id)
used to identify an individual participant (ie client etc).
This may be 'record_id,' 'pid', 'id' etc.
"""

history_help = """ 
This is the path that stores the history of mapping additions.
For each submission, as new participants are added to the data,
new records/mappings will be added. The "history" is a git repository
for each mapping. 
"""
versioned_filenames = {
    'shift_dates':'days_for_shift_date.csv',
    'replace_ids':'jdc_person_id.csv'
}
    
@click.command()
@click.option("--map-file-path",default="mappings.csv",help=map_file_help)
@click.option("--id-column",help=id_column_help)
@click.option("--history-dir",default=None,help=history_help)
def init_history_with_mapfile(map_file_path,id_column,history_dir):

    os.chdir(Path(map_file_path).parent)
    #open the previously made map file with days for shift date and jdc ids mapped to local ids 
    map_file_path = Path(map_file_path).resolve()
    map_df = pd.read_csv(map_file_path.with_suffix(".csv")).set_index(id_column)

    if not history_dir:
        history_dir = str(Path(map_file_path).resolve().with_suffix(""))+"-version-history"

    for fxn,filename in versioned_filenames.items():

        #init the git bare repo (ie version history directory)
        file_history_path = Path(history_dir)/Path(filename).with_suffix(".git")
        init_version_history(file_history_path)
        
        #commit the changes to a local repo and push these commits to the git bare repo
        with versioned_file_resource(filename,str(file_history_path), mode='a+') as f:
            if fxn=='replace_ids':
                series = map_df.filter(regex='jdc_person_id|submitter_id').squeeze()
                series.name = Path(filename).stem
            elif fxn=='shift_dates':
                series = map_df[Path(filename).stem]

            series.to_csv(f)

if __name__=="__main__":
    init_history_with_mapfile()



