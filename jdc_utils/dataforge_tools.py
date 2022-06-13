"""
From dataforge package:
Given only a few dataforge functions needed (if not using redcap), I included here to stream line workflow.

A set of general tools for use in working with data"""

import os
from pathlib import Path
import contextlib
from urllib.parse import urlparse
from git import Repo
from git.exc import NoSuchPathError
import random 
import pandas as pd
#from sys import platform # if you need to know which OS (window, linux, mac)

@contextlib.contextmanager
def versioned_file_resource(map_file, remote_url=None, repo_path=None, mode='r',
                            empty_ok=True):
    """Open versioned file and return corresponding file object
    
    Regular file object returned if remote_url and repo_path not specified.
    """

    #open the file object -- either with git control if specified or regular file
    if remote_url or repo_path:
        
        if remote_url and not repo_path:
            repo_name = os.path.basename(urlparse(remote_url).path)
            repo_path = Path(os.path.join('tmp/git', repo_name)).with_suffix('')
            print(f"Only remote specified so making local repo based off remote name called:\n{repo_path}")

        try:
            repo = Repo(repo_path)
            if remote_url:
                assert repo.remotes.origin.url == remote_url
            assert not repo.is_dirty(untracked_files=True)
            # Don't pull if remote is empty
            if repo.references: #shouldn't this be repo.references.remote?
                repo.remotes.origin.pull()
                print(f"Pulled in history from {remote_url} to {repo_path}")
            else:
                print("No references (because remote is empty) so not pulling anything from remote")
        except NoSuchPathError: #if repo_path specified but doesnt exist, than clone in the remote_url
            print(f"{repo_path} does not exist so cloning from {remote_url}")
            repo = Repo.clone_from(remote_url, repo_path)

        file_path = os.path.join(repo_path, map_file)
        #for windows, to avoid blank lines specified newline
        #https://docs.python.org/3/library/functions.html#open --> see universal newlines
        file_obj = open(file_path, mode,newline='') 
        print(f"Opening {file_path} (which is in a git repo)")
    else:
        repo = None
        file_path = map_file
        file_obj = open(map_file,mode,newline='')
        print(f"Opening {file_path} as a regular file object (no git repo)")



    #after opening the map file object -- either in the local repo path or a regular file object
    # finish starting the file context manager
    try:
        yield file_obj 
    
    except Exception as e:
        #if repo, need to reset to original state
        if repo:
            file_obj.close()
            repo.git.reset('--hard')
        raise e
    
    #closing the file object after stepping out of context manager
    finally:
        file_obj.close()
        if empty_ok and os.path.exists(file_path):
            file_size = os.path.getsize(file_path)
        else:
            file_size = True

        #remove empty/corrupt files?
        if not file_size:
            with contextlib.suppress(FileNotFoundError):
                os.remove(file_path)

        #if you have a local git repo with untracked files (ie your mapped file), add and commit and try to push 
        if repo and repo.is_dirty(untracked_files=True):
            repo.git.add('--all')
            repo.index.commit('Commit by versioned_file_resource()')
            try:
                repo.remotes.origin.push()

            #if pushing to remote does not work, then undo push and undo commit
            except Exception as e:
                repo.git.reset('--hard')
                repo.head.reset('HEAD~1', index=True, working_tree=True)
                raise e

def shift_dates(df,id_col,date_cols, map_file,map_url=None,shift_amount=365,keep_inputs=False):

    """Shift dates in a DataFrame by a random number of specified days 
    from a randomly determined uniform distribution and range
    and store this random factor in a file

    Can either append an entire new column of random shift date factors
    of an existing mapping file or
    add new ids and a random shift amount to the mapping file
    or do both.

    These options allow the flexibility to use with other de-identification
    mapping steps such as replace ids (either before or after the replace id step)

    TODO: Still need to review replace id function to ensure it can satisfy multiple non-id
    mapping columns. 

    The current implementation accounts for cases of:
    - ids but no random shifts
    - ids but missing values (this happens in the case of testing)
    - ids but more ids need to be added
    - no mappings file


    :params:
    df: input data file (pd.DataFrame)
    id_col: name of id column in dataframe and in map_file
    date_cols: the date column (str) or columns (list/iterable) to be shifted
    map_file: csv file where shift amount is stored and mapped to an id -- 
        this will be generated if it doesn't exist
    map_url: the git bare repository where history is stored
    shift_amount: the number of days to shift your date by
    level: if in index, this indicates which level ids are located (default is first level (0))
    column: name of column or index where ids are stored for dataframe
    keep_inputs: keep the amount shifted and the original input dates. This is False by default but 
        can be set to True for debugging/understanding how this function works.

    """   
    def _get_randint():
        shift_int = int(shift_amount/2)
        return random.randint(-shift_int,shift_int)

    date_cols = [date_cols] if isinstance(date_cols, str) else date_cols
    shift_col = 'days_for_shift_date'

    #get unique ids from df
    if id_col in df.columns:
        ids = list(set(df[id_col].to_list()))
    elif id_col in df.index.names:
        ids = list(set(df.index.get_level_values(id_col).to_list()))
    else:
        raise Exception("The specified id column -- {id_col} -- is not in the df")

    #check to make sure date columns are in the df
    for date_col in date_cols:
        assert date_col in df.columns or date_col in df.index.names
 
    #map/shift dates -- need two separate context managers as we are appending and/or adding columns to existing lines
    with versioned_file_resource(map_file, map_url, mode='r') as f:      
        f.seek(0)
        try:
            map = pd.read_csv(f)
            assert id_col in map.columns,'The specified id column was not found in mapping file'
            shift_list = pd.Series([_get_randint() for x in range(len(map))],index=map.index)
            if shift_col in map.columns:
                if map[shift_col].isna().sum()>0:
                    print("some ids have random shift amounts but some are empty so filling these")
                    map[shift_col].fillna(shift_list,inplace=True)
            else:
                print("the specified mapping file has ids but no random shift amounts so adding a a column of random shift amounts")
                map[shift_col] = shift_list

        except pd.errors.EmptyDataError:
            map = pd.DataFrame(columns = [id_col,shift_col])


        # find ids that werent in map file by merging dataset ids with mapped ids
        new_map = pd.Series(ids).rename(id_col).to_frame()
        new_map = new_map.merge(map, how='left', on=id_col, indicator=True,
                                  validate='one_to_one')
        new_map = new_map.loc[new_map['_merge'] == 'left_only', [id_col]].\
                            reset_index(drop=True)
        # add random int of range specified to ids that need it
        if len(new_map)>0:
            print('adding new random shift amounts')
            new_map[shift_col] = [_get_randint() for x in range(len(new_map))]
            new_map.sort_values(by=id_col, inplace=True)

            #append new map to the map df
            map = pd.concat([map,new_map])

    #save new mappings and date shift amounts to file -- write mode to save new columns and/or records
    with versioned_file_resource(map_file, map_url, mode='w') as f:
        map.to_csv(f,index=False)

    #perform the shift date operations
    with versioned_file_resource(map_file, map_url, mode='r') as f:    
        map = pd.read_csv(f)
        
        #df.reset_index(drop=False, inplace=True) #dont need to reset as merge fxn now searches index and columns
        df = df.merge(map[[id_col,shift_col]], how='left', on=id_col)


        #shift dates by specified number of days
        delta = df[shift_col].apply(lambda x: pd.Timedelta(days=x))
        if not keep_inputs:
            del df[shift_col]

        #shift dates
        for date_col in date_cols:
            dt = pd.to_datetime(df[date_col])
            df['shifted_' + date_col] = (dt + delta).dt.strftime('%Y%m%d')

            if not keep_inputs:
                del df[date_col]
        return df

# #%%
# mapping = r'C:\Users\kranz-michael\projects\rcg-bsd-gitlab\jcoin-maarc\jdc-utilities\tests\test-data\replace-id-tests\test-mappings.csv'
# df = pd.read_csv(r'C:\Users\kranz-michael\projects\rcg-bsd-gitlab\jcoin-maarc\jdc-utilities\tests\test-data\replace-id-tests\test-1-long-format.csv')
# # #%%
# df2 = shift_dates(df,'record_id','date_var',mapping)
# # # %%

