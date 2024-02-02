"""
Functions necessary to download and extract zip files from NCUA credit union data.

Contains:
    http_req_cred_zips() - Sends GET request to NCUA for report zip files.\n
    extract_cred_zips() - Extracts NCUA report zip files.\n
    download_cred_zips() - Downloads and extracts NCUA report files.\n
"""

import zipfile
from io import BytesIO
from os import PathLike

import requests
from requests import Response

from pipelineApplication.Helpers_FunctionsDicts import qtrMoNums
from pipelineApplication.bronzeLayer.DataRunParams import DataRunParams


def http_req_cred_zips(run_params: DataRunParams) -> Response:
    """
    Builds and sends GET request to NCUA URL for credit union reports in zip files.

    Returns:
         Response to GET request for credit union data from NCUA
    """
    qtr_month = qtrMoNums.get(f"Q{run_params.qtr}")
    qtr_year = run_params.qtrYear
    zip_url = (f"https://ncua.gov/files/publications/analysis/"
               f"call-report-data-{qtr_year}-{qtr_month}.zip")
    return requests.get(zip_url)


def extract_cred_zips(zip_in_bytes: bytes, rsc_dir_path: str | PathLike, run_params: DataRunParams):
    """
    Extracts the given zip file to a path built from the given path and run parameters.

    Args:
        zip_in_bytes: Content encoded in bytes in a zip file.
        rsc_dir_path: String or path for the temporary folder and file destination.
    """
    qtr_month = qtrMoNums.get(f"Q{run_params.qtr}")
    qtr_year = run_params.qtrYear
    zf = zipfile.ZipFile(BytesIO(zip_in_bytes))
    zf.extractall(f"{rsc_dir_path}/{qtr_year}{qtr_month}")


def download_cred_zips(rsc_dir_path: str | PathLike, run_params: DataRunParams):
    """
    Downloads and extracts credit union data reports from NCUA in a loop until point at which no report is available.

    Args:
        rsc_dir_path: String or path for the temporary folder and file destination.
        run_params: DataRunParams object managing parameters of the data pipeline job.
    """
    while True:
        filename = http_req_cred_zips(run_params).url.split('/')[-1]
        if http_req_cred_zips(run_params).status_code == 200:
            extract_cred_zips(http_req_cred_zips(run_params).content, rsc_dir_path, run_params)
            print(f"Extracting {filename}")
            run_params.increment_cred_params()
            continue
        else:
            print(f"Not yet available: {filename}")
            break
    print("Finished downloading credit union data.")
