"""Utilities for working with Gen3 Commons data dictionaries and corresponding submissions"""
import json 
import jsonschema
import requests
from frictionless import Resource,Schema
import pandas as pd
from pathlib import Path
# submission validation utilities for gen3 sheepdog validation 
def get_dictionary(endpoint,node_type='_all'):
    ''' 
    get json dictionary with all references (eg if $ref then API has these properties of this ref)
    
    _all gets entire schema while node names get only that node type
    ''' 
    api_url = f"{endpoint}/api/v0/submission/_dictionary/{node_type}"
    output = requests.get(api_url).text
    dictionary = json.loads(output)
    return dictionary

class Gen3Node:
    '''
    object represents a gen3 node with gen3 specific properties 
    and properties mapped to the frictionless schema specs for
    first-level, external validation before submitting records
    to JDC (for another level of validaiton). 

    TODO: [feature enhancement] make foreignKey as the link and another object as a package 
    (called Gen3Package) connecting all nodes of the graph model and have check to ensure
    all primaryKeys are also in the upstream foreignKey resource.

    Available methods:

    validate: uses frictionless spec to validate and returns error report
    submit: if error report is run and is valid, will submit to jdc
    delete: will delete all records for given program/project. 
    '''
    def __init__(self,endpoint,node_type,credentials_path=None):
        self.type = node_type
        dictionary = get_dictionary(endpoint,node_type) #note - full dictionary will be needed for node refs in future versions
        
        self.system_properties = dictionary['systemProperties']
        self.links = dictionary['links']
        self.required = dictionary['required']
        self.unique_keys = dictionary['uniqueKeys']
        #get just the link names
        self.link_names = [link["name"] for link in self.links if link.get("name")]

        if credentials_path:
            self.credentials_path = credentials_path

        self.schema = {"fields":[
            {
                "name":"type",
                "type":"string",
                "description":("The type (ie name) of the node "
                    "in the data common's graph model")
            }
        ],
        "primaryKey":["submitter_id"],
        "description":dictionary.get("description",""),
        "title":dictionary.get("title","")}
        
        #delete unnecessary field names in dictionary 
        for prop in self.system_properties:
            del dictionary['properties'][prop]
        del dictionary['properties']["type"] #hard coded above
        
        for propname,prop in dictionary["properties"].items():
            if propname in self.link_names and propname=="projects":
                frictionless_name = "projects.code"
            elif propname in self.link_names:
                frictionless_name = propname+".submitter_id"
            else:
                frictionless_name = propname
            frictionless_prop = {
                "name":frictionless_name,
                "title":prop.get("title",prop.get("label","")),
                "description":prop.get("description","")} 
    
            # type
            if prop.get("type"):
                if isinstance(prop["type"],list):
                    # NOTE: frictionless currently doesnt supprot field level missing vals
                    types = [t for t in prop["type"] if t!="null"]
                    # NOTE: frictionless doesnt support mixed types
                    if len(types) > 1:
                        frictionless_prop["type"] = "any"
                    else:
                        frictionless_prop["type"] = types[0]
                else:
                    frictionless_prop["type"] = prop["type"]
            else:
                # NOTE: again frictionless doesn't support mixed (ie oneOf)
                # enum without type will be any
                frictionless_prop["type"] = "any"

            # constraints
            frictionless_constraints = {}
            ## enum
            if prop.get("enum"):
                frictionless_constraints.update({"enum":prop["enum"]})
            ## required
            if propname in self.required:
                frictionless_constraints.update({"required":True})
            
            if frictionless_constraints:
                frictionless_prop["constraints"] = frictionless_constraints
            
            self.schema["fields"].append(frictionless_prop)

    def add_data(self,df):
        ''' 
        validates a pandas dataframe against node's frictionless schema. 
        
        Note,in sheepdog data model, record must be present in parent node.
        Since this doesn't account for parent node, wont detect.But see TODO
        in class docstring.
        '''
        cols = [p["name"] for p in self.schema["fields"]]
        def add_missing(df):
            missing = {col:None for col in cols if not col in df.columns}
            return df.assign(**missing)

        self.data = (
            df
            .reset_index()
            .assign(type=self.type)
            .pipe(lambda df:add_missing(df)) #add missing
            .filter(items=cols) #reorder field
            #.where(lambda df:df.notna(),None)#allows record to be kept when converting to json array (needed for fricitonless)
            .to_dict(orient="records")
        )
    def validate(self,df=pd.DataFrame()):
        if not df.empty:
            self.add_data(df)
        report = Resource(data=self.data,schema=self.schema).validate()
        return report
    def submit(self,df,program,project,credentials_path=None):
        """ validates (externally of commons)
        a pandas dataframe of transformed sheepdog records
        and if valid, will submit to the gen3 graph model 
        associated with the given credentials, program and project
        """ 
        from gen3.auth import Gen3Auth
        from gen3.submission import Gen3Submission

        if credentials_path:
            self.credentials_path = credentials_path
        sheepdog = Gen3Submission(Gen3Auth(refresh_file=self.credentials_path))

        self.add_data(df)

        if Resource(data=self.data,schema=self.schema).validate()['valid']:
            def unnest(df):
                nested_cols = [name for name in df.columns if len(name.split("."))>1]
                for name in nested_cols:
                    nested_name = name.split(".")
                    newname = nested_name.pop(0)
                    for _nested_name in nested_name:
                        df[name] = df[name].apply(lambda v:{_nested_name:v})
                    df.rename(columns={name:newname},inplace=True)
                return df

            records = pd.DataFrame(self.data).pipe(unnest).to_dict(orient="records")
                            
            self.sheepdog_record = sheepdog.submit_record(program, project, records)
        else:
            print(f"Node `{self.type}` not valid. run `node.resource.validate()` to see error report:")
    
    def delete(self,program,project,credentials_path=None):
        """ 
        delete all records for a node.
        This can be useful to ensure that any participants 
        dropped from dataset are not included in subsequent 
        dataset. Eg., after further quality control checking
        or data quarantining.

        The to-be deleted records are first saved to a file
        in path "tmp/sheepdog/<node_type>.json" in case re-upload/further
        examination necessary.
        """ 
        from gen3.auth import Gen3Auth
        from gen3.submission import Gen3Submission

        if credentials_path:
            self.credentials_path = credentials_path

        Path("tmp/sheepdog").mkdir(exist_ok=True,parents=True)
        sheepdog = Gen3Submission(Gen3Auth(refresh_file=self.credentials_path))
        sheepdog.export_node(
            program=program,
            project=project, 
            node_type=self.type,
            fileformat="json",
            filename=f"tmp/sheepdog/{self.type}.json")
        sheepdog.delete_node(program, project, self.type)
        

def map_to_sheepdog(
    sheepdog_dfs,
    endpoint,
    program,
    project,
    credentials_path,
    node_list=None,
    delete_first=True
    ):
    """
     Map a set of node records (formatted as a tabular pandas dataframe; 
        see gen3 TSV format for more details for tabular submissions)
    to sheepdog data commons data model.

    1. Loops through all nodes and validates data using fricitonless specs.
    2. Deletes current records (if param specified). This step is especially
        useful to ensure sheepdog records are in sync with other microservices
        using records (eg in file object storage such as indexd and MDS) and if 
        care needs to be taken to ensure no "quarantined" records are exposed inadvertently.
        > by default, will delete the node contents after exporting records for backup.
    3. Submits to sheepdog.

    sheepdog_dfs: dictionary of pandas DataFrames in hierarchical order of nodes
        (eg from parent to child nodes) with key being node_type (node name;eg participant, demographic etc)
            and value being pandas dfs.
    node_list: if specified, will only submit said node.
     Useful if submission errored out before completion and only need to submit a 
     subset of nodes.
    program: See gen3 sdk (authz program)
    project: See gen3 sdk (authz project)
    credentials_path: path to credentials json file (ie file downloaded from commons profile page)
    delete_first: Delete before submitting new records
    """
    sheepdog_params = dict(
        program=program,
        project=project,
        credentials_path=credentials_path
    )
    if node_list:
        initial_node_list = list(sheepdog_dfs)
        for node_name in initial_node_list:
            if not node_name in node_list:
                del sheepdog_dfs[node_name]

    # first level validation of new data
    for node_type,node_df in sheepdog_dfs.items():
        print(f"Validating records for {node_type}")
        node = Gen3Node(
            endpoint=endpoint,
            node_type=node_type
            )
        node.validate(df=node_df)
        if not node.validate()["valid"]:
            return node # return node and figure out why its not valid

    # delete old data: must happen from bottom-up as one cant delete records with child records
    if delete_first:
        bottom_up_list = reversed(list(sheepdog_dfs))
        for node_type in bottom_up_list:
            node = Gen3Node(endpoint, node_type)
            print(f"Deleting records for {node_type}")
            node.delete(**sheepdog_params)

    # submit new data
    for node_type,node_df in sheepdog_dfs.items():
        print(f"Submitting records for {node_type}")
        node = Gen3Node(
            endpoint=endpoint,
            node_type=node_type
            )
        node.submit(df=node_df,**sheepdog_params)
