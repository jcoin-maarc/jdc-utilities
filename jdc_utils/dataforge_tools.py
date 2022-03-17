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

def shift_dates(df, index_date, date_cols, merge_on=None):
    """Shift dates around an index date
    
    :param index_date: Dates around which dates in df will be shifted
    :type index_date: Series
    :param date_cols: Date column(s) in df to be shifted
    :param merge_on: Column(s) containing key(s) for merging index_date onto
        df; if None then df and index_date must have the same inde,optional

    """
    
    cols = df.columns.tolist()
    if merge_on:
        new_df = df.merge(index_date, how='left', on=merge_on,
                          validate='many_to_one')
    else:
        new_df = df.merge(index_date, how='left', left_index=True,
                          right_index=True, validate='many_to_one')
    
    date_cols = [date_cols] if isinstance(date_cols, str) else date_cols
    for col in date_cols:
        new_df[col] = new_df[col] - new_df[new_df.columns[-1]]
    
    return new_df[cols]

#pass get new index date -- random prob, if index already there (182)
#repo for dates
#local 