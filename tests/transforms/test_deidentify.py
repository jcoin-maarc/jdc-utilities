import os
from pathlib import Path

import git
import pandas as pd
from jdc_utils.transforms import deidentify

os.chdir(Path(__file__).parents[1])

df = pd.read_csv("data/test-1-long-format.csv")
mapping_file = Path("data/test_mappings.csv").resolve().as_posix()
history_path = Path(mapping_file).with_suffix("")
deidentify.init_version_history_all(history_path, overwrite=True)


def test_replace_ids():
    id_file = Path("data/test_ids.txt").resolve().as_posix()
    id_column = "record_id"

    # run replace id
    dfnew = deidentify.replace_ids(
        df=df, id_file=id_file, id_column=id_column, history_path=history_path
    )
    # check transformations and file outputs
    jdc_person_ids = ["C14-153", "C14-153", "C14-273", "C14-273", "C14-363"]

    combinedmappings = pd.read_csv(history_path.with_suffix(".csv")).to_dict(
        orient="records"
    )
    versioned_controlled = pd.read_csv(
        "tmp/git/jdc_person_id/jdc_person_id.csv"
    ).to_dict(orient="records")

    expected_combinedmappings = [
        {"record_id": 1, "jdc_person_id": "C14-153"},
        {"record_id": 2, "jdc_person_id": "C14-273"},
        {"record_id": 3, "jdc_person_id": "C14-363"},
    ]
    assert (dfnew["jdc_person_id"].values == jdc_person_ids).all()
    assert versioned_controlled == expected_combinedmappings
    assert combinedmappings == expected_combinedmappings

    # check version control history
    with git.Repo(history_path / "jdc_person_id.git") as r:
        assert len(list(r.iter_commits())) == 1


def test_shift_dates():
    dfnew = deidentify.shift_dates(
        df=df,
        id_column="record_id",
        date_columns="date_var",
        history_path=history_path,
        seed=2,
    )
    # check transformations
    versioned_controlled = pd.read_csv(
        "tmp/git/days_for_shift_date/days_for_shift_date.csv"
    ).to_dict(orient="records")

    shifted_dates = pd.Series(
        ["20230505", "20230505", "20221006", "20221006", "20220812"]
    )

    assert (dfnew["shifted_date_var"].values == shifted_dates).all()
    assert versioned_controlled == [
        {"record_id": 1, "days_for_shift_date": 124},
        {"record_id": 2, "days_for_shift_date": -87},
        {"record_id": 3, "days_for_shift_date": -142},
    ]
    # check version control history
    with git.Repo(history_path / "days_for_shift_date.git") as r:
        assert len(list(r.iter_commits())) == 1


if __name__ == "__main__":
    try:
        os.chdir(Path(__file__).parents[1])
        test_replace_ids()
        test_shift_dates()
    finally:
        git.rmtree("data/test_mappings")
        git.rmtree("tmp/git")
        os.rmdir("tmp")
        os.remove("data/test_mappings.csv")
