import shutil
from os import PathLike
import json

from pyspark.sql import functions as f
from pyspark.sql.column import Column
from pyspark.sql.dataframe import DataFrame
from requests import Response

qtrMoNums = {"Q1": "03",
             "Q2": "06",
             "Q3": "09",
             "Q4": "12"}
dateToQtrDict = {"03-31": "1",
                 "06-30": "2",
                 "09-30": "3",
                 "12-31": "4"}


def write_bank_json_resources(data: Response, rsc_dir_path: str | PathLike, fin_or_inst: str):
    writer = open(f"{rsc_dir_path}/{fin_or_inst}.json", "a")
    json_resp = data.json()["data"]
    json.dump(json_resp, writer)


def check_columns_for_null(df: DataFrame):
    for col in df.columns:
        print(f"Checking {col} for null values")
        df.filter(f"{col} is NULL").show(truncate=False)


def value_via_dict(mapping_broadcasted):
    def g(x):
        return mapping_broadcasted.value.get(x)

    return f.udf(g)


def select_sort_dated_cols(df: DataFrame, first_cols: list[str | Column], sort_desc: bool):
    def g():
        rem_cols: list[str] = []
        for col in df.columns:
            if col not in first_cols:
                rem_cols.append(col)
        return rem_cols

    new_select_order = first_cols + sorted(g(), reverse=sort_desc)
    return df.select(new_select_order)


def delete_downloaded_resources():
    print("Deleting temporarily downloaded data sources...")
    shutil.rmtree("./resources")
    print("Temporary downloaded files and resources successfully deleted.")
