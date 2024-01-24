import zipfile
from io import BytesIO
from os import PathLike

import requests

from pipelineApplication.Helpers_FunctionsDicts import qtrMoNums
from pipelineApplication.bronzeLayer.DataRunParams import DataRunParams, increment_cred_params


def http_req_cred_zips():
    qtr_month = qtrMoNums.get(f"Q{DataRunParams.qtr}")
    qtr_year = DataRunParams.qtrYear
    zip_url = (f"https://ncua.gov/files/publications/analysis/"
               f"call-report-data-{qtr_year}-{qtr_month}.zip")
    return requests.get(zip_url)


def extract_cred_zips(zip_in_bytes, rsc_dir_path: str | PathLike):
    qtr_month = qtrMoNums.get(f"Q{DataRunParams.qtr}")
    qtr_year = DataRunParams.qtrYear
    zf = zipfile.ZipFile(BytesIO(zip_in_bytes))
    zf.extractall(f"{rsc_dir_path}/{qtr_year}{qtr_month}")


def download_cred_zips(rsc_dir_path: str | PathLike):
    while True:
        filename = http_req_cred_zips().url.split('/')[-1]
        if http_req_cred_zips().status_code == 200:
            extract_cred_zips(http_req_cred_zips().content, rsc_dir_path)
            print(f"Extracting {filename}")
            increment_cred_params()
            continue
        else:
            print(f"Not yet available: {filename}")
            break
    print("Finished downloading credit union data.")
