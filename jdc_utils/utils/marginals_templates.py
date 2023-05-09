import random

import pandas as pd


# utilities for collecting aggregated data from templates
def split_marginals_and_total(df: pd.DataFrame) -> pd.DataFrame:
    """split into total and marginals

    Args:
        df (pd.DataFrame): _description_

    Returns:
        tuple[pd.DataFrame,pd.DataFrame]: _description_
    """
    totals = df.loc[df.index == "Total"]
    marginals = df.loc[df.index != "Total"]
    totals_diff = totals - marginals.sum()
    totals_exceed = totals_diff < 0
    # check totals
    exit = False
    if totals_exceed.sum().sum() < 0:
        mess = (
            f"For {sheet_name}:\n"
            "the sum of marginals exceed the reported total number\n"
        )
        exit = True

    if exit:
        sys.exit(mess)

    # add not reported (happens if cell size restrictions)

    try:
        not_reported = marginals.loc["Not reported", :]
        not_reported += totals_diff.values.flatten()
    except KeyError:
        marginals.loc["Not reported", :] = totals_diff.values.flatten()

    return marginals


# create individual level data


# multiply category by count
def create_marginal_sample(
    marginals: pd.DataFrame,
    column_name: str = "column",
    category_name: str = "category",
) -> pd.DataFrame:
    """Create the marginal sample (or individual level data)
    by multiplying the category names by the total count for that
    category


    Args:
        marginal (pd.DataFrame): _description_
        column_name (str): _description_
        category_name (str): _description_

    Returns:
        pd.DataFrame: _description_
    """
    sample = []
    for col, marginal in marginals.iteritems():
        for category, total in marginal.iteritems():
            if pd.notna(total):
                # shuffle within each group (ie col or column -- ie type of participant)
                col_sample = int(total) * [{column_name: col, category_name: category}]
                random.shuffle(col_sample)
                sample.extend(col_sample)

    return pd.DataFrame(sample)


def _create_sample(xls_path, sheet_name):
    sample_df = (
        pd.read_excel(xls_path, sheet_name=sheet_name)
        .pipe(lambda df: df.set_index(df.columns[0]))
        .pipe(split_marginals_and_total)
        .pipe(create_marginal_sample)
        .pipe(lambda df: df.set_index(df.columns[0], append=True))
    )
    return sample_df


def convert_marginals_template_to_sample(
    xls_path: str, sheets_to_skip=["Instructions", "README"]
) -> pd.DataFrame:
    with pd.ExcelFile(xls_path, engine="openpyxl") as f:
        marginals = [
            _create_sample(f, sheet_name).squeeze().rename(sheet_name)
            for sheet_name in f.sheet_names
            if not sheet_name in sheets_to_skip
        ]

    return pd.concat(marginals, axis=1)
