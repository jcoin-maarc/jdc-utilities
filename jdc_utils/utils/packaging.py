import shutil
from pathlib import Path
from zipfile import ZipFile


# general utilities
def copy_file(file_path, target_path):
    with open(file_path, "rb") as source:
        with open(target_path, "wb") as target:
            shutil.copyfileobj(source, target)

def _write_to_zip(input_path,zip_object):
    '''
    recursively writes file contents
    of a directory
    to a zip object (in write mode)

    TODO: write directories (so can write empty dirs) without using mkdir
    '''       
    input_path = Path(input_path).resolve()
    input_contents = input_path.glob("*")
    
    for path in input_contents:
        #relative directory to write dir/file within zip
        relative_path = path.relative_to(input_path)
        if path.is_file():
            zip_object.write(path,relative_path)
        else:
            #zip_object.mkdir(relative_dir) #ZipFile.mkdir only supported starting in python 3.11
            _write_to_zip(path,zip_object)
# packaging
def zip_package(pkg_path,zip_path):
    ''' 
    takes a valid package and outputs 
    to a zipped file.

    package-name --> package-name.zip
    '''
    assert Path(pkg_path).is_dir()
    assert Path(zip_path).is_dir()

    pkg_path = Path(pkg_path).resolve()
    outzip_path = (Path(zip_path).resolve()/
        pkg_path.with_suffix('.zip').name)

    with ZipFile(outzip_path,'w') as pkg_zip:
        _write_to_zip(pkg_path,pkg_zip)

    return outzip_path