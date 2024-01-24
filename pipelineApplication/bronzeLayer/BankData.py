import requests

from pipelineApplication.bronzeLayer.DataRunParams import DataRunParams, currentDate

# Setup API calls
apiBaseURL = "https://banks.data.fdic.gov/api"
headers = {'Accept': 'application/json; charset=utf-8'}


def inst_filters(start_cert, stop_cert):
    return f"CERT:[{start_cert} TO {stop_cert}]"


def inst_query(start_cert=0, stop_cert=9999):
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
    return inst_req


def fin_filters(start_cert=0, stop_cert=9999):
    return (f"CERT:[{start_cert} TO {stop_cert}] AND "
            f"REPDTE:[{DataRunParams.prevRun} TO {currentDate}]")


def fin_query(start_cert=0, stop_cert=9999):
    fin_fields = "CERT,REPDTE,ASSET,DEP"
    fin_params = {'filters': fin_filters(start_cert, stop_cert),
                  'fields': fin_fields,
                  'sort_by': 'CERT',
                  'sort_order': 'ASC',
                  'limit': 10000,
                  'format': 'json',
                  'download': 'false'}
    inst_req = requests.get(f"{apiBaseURL}/financials", params=fin_params)
    if inst_req.status_code == 200:
        print("Received FDIC financial data.")
    else:
        print(f"Malfunction requesting institutional data from FDIC\n"
              f"Error Code: {inst_req.status_code}")
    return inst_req
