"""Utilities for working with Gen3 Commons data dictionaries and corresponding submissions"""
import json 
import jsonschema
import requests
from frictionless import Resource,Schema

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
    and standard json schema properties for:
        1. validating records
        2. writing a validated set of records to specified file format
        3. node property look up
    '''
    def __init__(self,endpoint,type,credential_path=None):
        self.type = type
        dictionary = get_dictionary(endpoint,type) #note - full dictionary will be needed for node refs in future versions
        
        self.system_properties = dictionary['systemProperties']
        self.links = dictionary['links']
        self.required = dictionary['required']
        self.unique_keys = dictionary['uniqueKeys']
        #get just the link names
        self.link_names = [link["name"] for link in self.links if link.get("name")]

        if credential_path:
            self.credential_path = credential_path

        self.frictionless_schema = {"fields":[],
        "primaryKey":["submitter_id"],
        "description":dictionary.get("description"),
        "title":dictionary.get("title")}
        
        for prop in self.system_properties:
            del dictionary[prop]
        
        for propname,prop in list(dictionary):
            frictionless_prop = {
                "name":propname,
                "description":propname.get("description")} 
    
            # type
            if prop.get("type"):
                if isinstance(prop["type"],list):
                    # NOTE: frictionless currently doesnt supprot field level missing vals
                    types = [t for t in prop["type"] if t!="null"]
                    # NOTE: frictionless doesnt support mixed types
                    if len(types) > 1:
                        frictionless_prop["type"] = "any"
                    else:
                        frictionless_prop["type"] = prop["type"]
            else:
                # NOTE: again frictionless doesn't support mixed (ie oneOf)
                # enum will be any
                frictionless_prop["type"] = "any"

            # constraints
            frictionless_constraints = {}
            ## enum
            if prop.get("enum"):
                frictionless_contraints.update({"enum":prop["enum"]})
            ## required
            if propname in self.required:
                frictionless_constraints.update({"required":True})
            
            if frictionless_constraints:
                frictionless_prop.update(frictionless_constraints)
            
            frictionless_schema["fields"].append(frictionless_prop)

    def validate(self,df):
        ''' 
        validates against node's frictionless schema. Note,
        in sheepdog data model, record must be present in parent node.
        Since this doesn't account for parent node, wont detect.
        '''
        cols = [p.get("name") for p in self.frictionless_schema["fields"]]
        data = df.filter(items=cols) #reorder fields
        if "submitter_id" in data.columns:
            data.set_index("submitter_id",inplace=True)
        report = Resource(data,schema=Schema(self.frictionless_schema)).validate()
        return report
    def submit(self,df,program,project,credential_path=None):
        """ validates (externally of commons)
        a pandas dataframe of transformed sheepdog records
        and if valid, will submit to the gen3 graph model 
        associated with the given credentials, program and project
        """ 
        from gen3.auth import Gen3Auth
        from gen3.submission import Gen3Submission

        if credential_path:
            self.credential_path = credential_path

        sheepdog = Gen3Submission(Gen3Auth(self.credential_path))
        report= self.validate(df)
        if report["valid"]:
            records = df.reset_index("submitter_id").to_dict(orient="records")
            return sheepdog.submit_record(program, project, records)
        else:
            print(f"Node `{self.type} not valid. See report below:")
            raise Exception(report.to_summary())
    
    def delete(self,program,project,credentials_path=None):
        """ 
        delete all records for a node.
        This can be useful to ensure that any participants 
        dropped from dataset are not included in subsequent 
        dataset. Eg., after further quality control checking
        or data quarantining.
        """ 
        from gen3.auth import Gen3Auth
        from gen3.submission import Gen3Submission

        if credential_path:
            self.credential_path = credential_path

        sheepdog = Gen3Submission(Gen3Auth(self.credential_path))
        sheepdog.delete_node(program, project, self.type)
        




