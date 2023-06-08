import os
import shutil
from pathlib import Path
from zipfile import ZipFile


# general utilities
def copy_file(file_path, target_path):
    with open(file_path, "rb") as source:
        with open(target_path, "wb") as target:
            shutil.copyfileobj(source, target)


def _write_to_zip(input_path, zip_object):
    """
    recursively writes file contents
    of a directory
    to a zip object (in write mode)

    TODO: write directories (so can write empty dirs) without using mkdir
    """
    input_path = Path(input_path).resolve()
    input_contents = input_path.glob("*")

    for path in input_contents:
        # relative directory to write dir/file within zip
        relative_path = path.relative_to(input_path)
        if path.is_file():
            zip_object.write(path, relative_path)
        elif path.is_dir():
            zip_object.mkdir(relative_path)
        else:
            raise Exception("Something went wrong with zipping")


# packaging
def read_package(filepath):
    """
    reads in file path which can either be a directory containing
    a data package descriptor or resources (ie data files). This can
    also include glob regular expressions (compatible with frictionless Package)
    and converts all resource data to pandas dataframes
    """

    # NOTE for code below: frictionless security doesn't play well with particular paths
    # see: https://specs.frictionlessdata.io/data-resource/#data-location
    pwd = os.getcwd()
    filename = Path(filepath).name
    os.chdir(Path(filepath).parent)
    if Path(filename).is_dir():
        os.chdir(filename)
        if Path("data-package.json").is_file():
            package = Package("data-package.json")
        elif Path("datapackage.json").is_file():
            package = Package("datapackage.json")
        else:
            package = Package("*")
    else:
        package = Package(filename)

    print(os.getcwd())

    # has data package
    # has a baseline and timepoints resource
    package_pandas = Package()
    for resource in package.resources:
        try:
            name = resource.name
            data = resource.to_petl().todf()
            resource_pandas = Resource(data, name=name)
            package_pandas.add_resource(resource_pandas)
        except:
            print(f"In {filepath}")
            print(f"Something went wrong when loading {name}")
            print(f"Removing {name} from the source package")
            del resource

    os.chdir(pwd)  # NOTE: change dir back to original dir for other steps
    return package_pandas


def zip_package(pkg_path, zip_path):
    """
    takes a valid package and outputs
    to a zipped file.

    package-name --> package-name.zip
    """
    assert Path(pkg_path).is_dir()
    assert Path(zip_path).is_dir()

    pkg_path = Path(pkg_path).resolve()
    outzip_path = Path(zip_path).resolve() / pkg_path.with_suffix(".zip").name

    with ZipFile(outzip_path, "w") as pkg_zip:
        input_contents = Path(pkg_path).resolve().glob("*")
        for root, _, files in os.walk(pkg_path):
            for file in files:
                file_path = os.path.join(root, file)
                # relative directory to write dir/file within zip
                relative_path = Path(file_path).relative_to(pkg_path)
                pkg_zip.write(file_path, relative_path)
    return outzip_path
