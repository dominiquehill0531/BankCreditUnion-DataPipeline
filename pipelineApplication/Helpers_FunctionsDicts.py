"""
Functions and dicts used throughout the pipeline application that are not exclusive to any one layer or file.

Contains:
    moNumQtrs: dict - Correlates month number to financial quarter number. \n
    qtrMoNums: dict - Correlates financial quarter key ("Q#") to number of month value string ("MM").\n
    dateToQtrDict: dict - Correlates financial quarter end dates ("MM-DD") to quarter number value string ("#").\n
    write_bank_json_resources() - Writes temp JSON files.\n
    check_columns_for_null() - Iterates columns in DataFrame checking for null values.\n
    value_via_dict() - Operates as get() for broadcasted dict.\n
    select_sort_dated_cols() - Sorts all but chosen columns in DataFrame.\n
    delete_downloaded_resources() - Deletes temp directory and files in pipeline process.\n
"""

import json
import shutil
from os import PathLike

from pyspark.broadcast import Broadcast
from pyspark.sql import functions as f
from pyspark.sql.column import Column
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.udf import UserDefinedFunction
from requests import Response

moNumQtrs = {1 | 2 | 3: 1,
             4 | 5 | 6: 2,
             7 | 8 | 9: 3,
             10 | 11 | 12: 4}
qtrMoNums = {"Q1": "03",
             "Q2": "06",
             "Q3": "09",
             "Q4": "12"}
dateToQtrDict = {"03-31": "1",
                 "06-30": "2",
                 "09-30": "3",
                 "12-31": "4"}


def write_bank_json_resources(data: Response, rsc_dir_path: str | PathLike, fin_or_inst: str):
    """
    Writes temporary JSON files of downloaded bank information to a temporary folder within the application.

    Args:
        data: Response to a GET request returning content in JSON format.
        rsc_dir_path: String or path for the temporary folder and file destination.
        fin_or_inst: String designating whether the content is financial ("fin") or institutional ("inst") data.
    """
    writer = open(f"{rsc_dir_path}/{fin_or_inst}.json", "a")
    json_resp = data.json()["data"]
    json.dump(json_resp, writer)


def check_columns_for_null(df: DataFrame):
    """
    Iterates through given DataFrame columns, showing records with null or empty values for development purposes.

    Args:
        df: DataFrame with columns to check.
    """
    for col in df.columns:
        print(f"Checking {col} for null values")
        df.filter(f"{col} IS NULL OR {col} = ''").show(truncate=False)


def value_via_dict(mapping_broadcasted: Broadcast) -> UserDefinedFunction:
    """
    Defines a function allowing the use of a dict object by a distributed cluster to assign values.

    Args:
        mapping_broadcasted: Dict made available to a distributed cluster by SparkContext.broadcast().

    Returns:
        User-defined function returning values based on a given dict and column values as keys.
    """

    def g(x):
        return mapping_broadcasted.value.get(x)

    return f.udf(g)


def select_sort_dated_cols(df: DataFrame, first_cols: list[str | Column], sort_desc: bool = True) -> DataFrame:
    """
    Sorts columns in DataFrame excluding those in the given list, which will show first in list order.

    Args:
        df: DataFrame with columns to organize.
        first_cols: List of columns NOT to be sorted.
        sort_desc: Boolean designating whether columns will be sorted in descending order; defaults True.

    Returns:
        DataFrame with selected unsorted columns trailed by sorted columns.
    """
    def g():
        rem_cols: list[str] = []
        for col in df.columns:
            if col not in first_cols:
                rem_cols.append(col)
        return rem_cols

    new_select_order = first_cols + sorted(g(), reverse=sort_desc)
    return df.select(new_select_order)


def delete_downloaded_resources():
    """
    Deletes the directory and included files designated for temporary data staging in the Bronze Layer.
    """
    print("Deleting temporarily downloaded data sources...")
    shutil.rmtree("./resources")
    print("Temporary downloaded files and resources successfully deleted.")
