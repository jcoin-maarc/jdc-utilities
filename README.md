# Utilities for preparing and submitting data to the JDC

## Extracting summary statistics

**IN DEVELOPMENT**

## Proposed workflow for submitting individual level data

This workflow provides a scalable way to manage different hub data collection procedures and general enough that many of these elements can be generalized to other use cases. 

Only changes to human-readable files are required (eg the config and transform yaml files)

**See examples/example.py  for an example implemention of this workflow containing example config and transform yaml files with datafiles 
and output tsv files.**


### User-specified files
1. config.yaml

Intended to be the only input file for CLI. Consists of records with the following parameters for each record:

```yaml
    - program_name: <name of project code that ids project>
    hub_name: <name of project that ids project>
    variable_categories: <list of node names that variables desired to be submitted come from. This is called variable categories to make more understandable by user.>
    data_file_path: <path to data file (eg csv, excel etc)>
    variable_transform_file_path: <path to yaml file that specifies transformation (eg variable renaming, value remapping, other transformations to conform to JDC properties)>
```

So, say a hub is updating data to the JDC and has separate data collection systems (and output data files) for clients and staff data for fields in the participant and demographic nodes (ie variable categories), this would have 2 records in the config.yaml:

```yaml
datafiles:
    - program_name: JCOIN
    hub_name: TEST
    variable_categories: 
        - participant
        - demographic
    data_file_path: /Users/johndoe/project/staff_local_data.xlsx
    variable_transform_file_path: /Users/johndoe/project/staff_transforms.yaml
    - program_name: JCOIN
    hub_name: TEST
    variable_categories: 
        - participant
        - demographic
    data_file_path: /Users/johndoe/project/client_local_data.xlsx
    variable_transform_file_path: /Users/johndoe/project/client_transforms.yaml
```
*Note: the records are nested under the `datafiles` name to provide option for other configurations (eg specifying base directory)*

2. transforms.yaml

Specifies the function, paramater name and parameter input
to be run on input data in a yaml file:

```yaml
<name of registered/valid pandas function>:
    <name of function parameter>: <paramter input (this may be a string, list, or dictionary)>
```

Examples:

Example native pandas functions :

*Rename columns:*

```yaml
rename:
    columns:
        d4_b: gender
        record_id: submitter_id
```

is intended to run:

```python
df.rename(columns={
    'd4_b':'gender',
    'record_id':'submitter_id'
})

```
Custom registered functions leverage pandas-flavor 
module to directly make pandas methods to the 
pd.DataFrame class. Many of these registered
functions are in the transforms.py, but does not necessarily
need to be restricted to only this file.

Example custom registered functions:

*Rename columns:*
```yaml
rename_columns:
  from_name_to_name:
    d4_b: gender
    record_id: submitter_id
```
is intended to run the function

```python
df.rename_columns(from_name_to_name={
    'd4_b':'gender',
    'record_id':'submitter_id'
})
```


*Convert a date field to a new quarter field:*

```yaml
to_quarter:
    from_date_name_to_quarter_name:
        date_recruited: quarter_recruited
```

is intended to run the function

```python
df.to_quarter(from_date_name_to_quarter_name={'date_recruited':'quarter_recruited'})
```

### Workflow 

1. Read in data file to a data frame
    - `transforms.read_df(filepath)` function from specified data file path in the config.yaml file
2. Transform data frame to JDC properties and values
    - `transforms.run_transformfile(df,transformfile)` function from the transform functions specified in the transforms.yaml file (which itself is specified in the config.yaml file) on the dataframe
3. Validate the transformed data for each variable category (node) and save to a tsv
    - initiate `submisson.Node(type)` instance with the node type (ie variable category) 
    - run `submission.Node.to_tsv(df)`
        - will either receive feedback on what you need to add to the transform file or will save to tsv either in a file or in memory for uploading to JDC
        - Note: details of this step still in development
4. Directly upload to gen3?
    - may be easier to have a workspace dedicated to this (and all JDC submission procedures?)