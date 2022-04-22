**THIS README IS A WORK IN PROGRESS!**

# Utilities for de-identifying data, transforming data, and validating data for JDC submissions



## Proposed workflow for submitting individual level data

The idea behind this version of the jdc utilities is three-fold. 

1. To provide tools to build a stand-alone data package (that includes metadata and data) via the frictionless framework and software suite of tools.
2. To expose a command line interface to make these tools easier to use and easier to understand intermediate steps. For example, to see how 
the replace id function has replaced ids, to compare the shifted to non shifted dates, and finally to view the transformed dataset that either passed
validation (ie conformed to the given data model or did not). 
3. To provide tools to automatically go from this stand-alone (but harmonized) data package into the JDC data model for quick exploration of important variables.


This workflow provides a scalable way to manage different hub data collection procedures and general enough that many of these elements can be generalized to other use cases. 

## Options for using tools

There are several options to use these jdc utilities.

1. Use these tools directly in python scripts.
2. Use the command line interface with all the individual parameters.
3. Use the command line interface with configuration files that store the input parameters.


## Workflow steps

The output of each workflow is saved in order to understand the intermediate steps. 

TODO: CLI function that runs all steps at once

### replace ids
<add text here>

### shift dates
<add CLI help text>

### transform data
Transforming data currently leverages functions written with pandas and pandas-flavor decorator.

It is a simple two step process:

1. Read in data file to a data frame
    - `transforms.read_df(filepath)` function from specified data file path 
2. Transform data frame to JDC properties and values
    - `transforms.run_transformfile(df,transformfile)` function from the transform functions specified in a transforms.yaml file (which itself is specified in the config.yaml file) on the dataframe


This transformfile contains a way to specify transforms within a simple text file called a "yaml" file:

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


### validate data
<add text about frictionless validation>

Also supports validation of JDC data model
1. Validate the transformed data for each variable category (node) and save to a tsv
    - initiate `submisson.Node(type)` instance with the node type (ie variable category) 
    - run `submission.Node.to_tsv(df)`
        - will either receive feedback on what you need to add to the transform file or will save to tsv either in a file or in memory for uploading to JDC
        - Note: details of this step still in development
2. Directly upload to gen3?
    - may be easier to have a workspace dedicated to this (and all JDC submission procedures?)
