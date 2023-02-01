from jdc_utils.io import read_df

def read_transformfile(transformfile):
    with open(transformfile) as file:
        return yaml.safe_load(file)


def run_transformfile(df, transformfile):
    """
    loop through transformations and mappings as specified
    in the yaml file

    then runs a given function name with a set of paramaters.
    Intended to transform the dataframe inplace.
    To provide compatability with native pandas fxns,
    the inplace argument assumed to be a parameter.

    If kwargs, then need to register a function
    calling the dictionary as keyword args in TransformDf class.
    """
    transform_mappings = OrderedDict(read_transformfile(transformfile))

    for fxn_name, params in transform_mappings.items():
        if fxn_name in ['replace_ids','shift_dates']:
            print(fxn_name)
            df = getattr(df, fxn_name)(**params)
            print(df.columns)
        else:
            getattr(df, fxn_name)(**params)
    
    return df 