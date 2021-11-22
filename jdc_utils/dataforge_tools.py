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

@contextlib.contextmanager
def versioned_file_resource(file, remote_url=None, repo_path=None, mode='r',
                            emptyok=False):
    """Open versioned file and return corresponding file object
    
    Regular file object returned if remote_url and repo_path not specified.
    """
    
    repo = None
    if remote_url or repo_path:
        
        if remote_url and not repo_path:
            repo_name = os.path.basename(urlparse(remote_url).path)
            repo_path = Path(os.path.join('tmp/git', repo_name)).with_suffix('')
        
        try:
            repo = Repo(repo_path)
            if remote_url:
                assert repo.remotes.origin.url == remote_url
            assert not repo.is_dirty(untracked_files=True)
            # Don't pull if remote is empty
            if repo.references:
                repo.remotes.origin.pull()
        except NoSuchPathError:
            repo = Repo.clone_from(remote_url, repo_path)
    
    filepath = os.path.join(repo_path, file)
    file_obj = open(filepath, mode)
    
    try:
        yield file_obj
    
    except Exception as e:
        if repo:
            file_obj.close()
            repo.git.reset('--hard')
        raise e
    
    finally:
        file_obj.close()
        if not emptyok and os.path.exists(filepath):
            flen = os.path.getsize(filepath)
        else:
            flen = True
        if not flen:
            with contextlib.suppress(FileNotFoundError):
                os.remove(filepath)
        if repo and repo.is_dirty(untracked_files=True):
            repo.git.add('--all')
            repo.index.commit('Commit by versioned_file_resource()')
            try:
                repo.remotes.origin.push()
            except Exception as e:
                repo.git.reset('--hard')
                repo.head.reset('HEAD~1', index=True, working_tree=True)
                raise e

def shift_dates(df, index_date, date_cols, merge_on=None):
    """Shift dates around an index date
    
    :param index_date: Dates around which dates in df will be shifted
    :type index_date: Series
    :param date_cols: Date column(s) in df to be shifted
    :type date_cols: str or list
    :param merge_on: Column(s) containing key(s) for merging index_date onto
        df; if None then df and index_date must have the same index
    :type merge_on: str or list, optional
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
