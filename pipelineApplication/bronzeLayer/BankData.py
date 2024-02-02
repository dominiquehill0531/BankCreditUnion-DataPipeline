"""
Dicts, strings, and functions necessary to query for bank information from the FDIC API.

Contains:
    apiBaseURL: str - Is the URL for the FDIC's API public endpoint.\n
    headers: dict - Designates format of content received in API response.\n
    inst_filters() - Creates string representing filter for institutional FDIC query.\n
    inst_query() - Sends GET request to FDIC API for institutional data.\n
    fin_filters() - Creates string representing filters for financial FDIC query.\n
    fin_query() - Sends GET request to FDIC API for financial data.\n
"""

import requests
from requests import Response

from pipelineApplication.bronzeLayer.DataRunParams import DataRunParams

# Setup API calls
apiBaseURL = "https://banks.data.fdic.gov/api"
headers = {'Accept': 'application/json; charset=utf-8'}


def inst_filters(start_cert: int, stop_cert: int) -> str:
    """
    Creates string for use in dict of filter parameters for API institutional query.

    Args:
        start_cert: Integer representing the charter number from which to start the FDIC query.
        stop_cert: Integer representing the charter number with which to end the FDIC query.

    Returns:
         String used in FDIC API query to designate range of charter numbers to search.
    """
    return f"CERT:[{start_cert} TO {stop_cert}]"


def inst_query(start_cert: int = 0, stop_cert: int = 9999) -> Response:
    """
    Builds and sends GET request to FDIC API for institutional data in JSON format.

    Args:
        start_cert: Integer representing the charter number from which to start the FDIC query.
        stop_cert: Integer representing the charter number with which to end the FDIC query.

    Returns:
        inst_req: Response to GET request for institutional data from FDIC API with JSON.

    Raises:
        RuntimeError if API response status code is not 200, successful.
    """
    inst_fields = "CERT,REPDTE,NAME,CITY,STNAME,WEBADDR,ACTIVE"
    inst_params = {'filters': inst_filters(start_cert, stop_cert),
                   'fields': inst_fields,
                   'sort_by': 'CERT',
                   'sort_order': 'ASC',
                   'limit': 10000,
                   'format': 'json',
                   'download': 'false'}
    inst_req = requests.get(f"{apiBaseURL}/institutions", params=inst_params)
    if inst_req.status_code == 200:
        print("Received FDIC institutional data.")
    else:
        print(f"Malfunction requesting institutional data from FDIC\n"
              f"Error Code: {inst_req.status_code}")
        raise RuntimeError("Problem requesting institutional bank data from FDIC API. "
                           "Check for updates or changes in querying process at https://banks.data.fdic.gov/docs/.")
    return inst_req


def fin_filters(run_params: DataRunParams, start_cert: int = 0, stop_cert: int = 9999):
    """
   Creates string for use in dict of filter parameters for API financial query.

   Args:
       run_params: DataRunParams object managing parameters of the data pipeline job.
       start_cert: Integer representing the charter number from which to start the FDIC query.
       stop_cert: Integer representing the charter number with which to end the FDIC query.

   Returns:
        String used in FDIC API query to designate range of charter numbers to search.
   """
    return (f"CERT:[{start_cert} TO {stop_cert}] AND "
            f"REPDTE:[{run_params.prevRun} TO {run_params.currentRun}]")


def fin_query(run_params: DataRunParams, start_cert: int = 0, stop_cert: int = 9999):
    """
    Builds and sends GET request to FDIC API for financial data in JSON format.

    Args:
        run_params: DataRunParams object managing parameters of the data pipeline job.
        start_cert: Integer representing the charter number from which to start the FDIC query.
        stop_cert: Integer representing the charter number with which to end the FDIC query.

    Returns:
        fin_req: Response to GET request for financial data from FDIC API with JSON.

    Raises:
        RuntimeError if API response status code is not 200, successful.
    """
    fin_fields = "CERT,REPDTE,ASSET,DEP"
    fin_params = {'filters': fin_filters(run_params, start_cert, stop_cert),
                  'fields': fin_fields,
                  'sort_by': 'CERT',
                  'sort_order': 'ASC',
                  'limit': 10000,
                  'format': 'json',
                  'download': 'false'}
    fin_req = requests.get(f"{apiBaseURL}/financials", params=fin_params)
    if fin_req.status_code == 200:
        print("Received FDIC financial data.")
    else:
        print(f"Malfunction requesting financial data from FDIC\n"
              f"Error Code: {fin_req.status_code}")
        raise RuntimeError("Problem requesting financial bank data from FDIC API. "
                           "Check for updates or changes in querying process at https://banks.data.fdic.gov/docs/.")
    return fin_req
