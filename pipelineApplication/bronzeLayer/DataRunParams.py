from datetime import datetime

currentDate = datetime.today().strftime('%Y-%m-%d')
prevRunStr = "2023-06-30"
currentMo = datetime.today().strftime('%m')


class DataRunParams:  # TODO: Create prevRun log file to autoincrement defaults
    # Default changes with each run
    currentRun = currentDate
    prevRun = prevRunStr
    qtrYear = 2023
    qtr = 1
    # Default remains
    certNumStart = 0
    certNumStop = 9999
    maxCerts = 100000  # No more than 100K Bank certs

    def to_str(self):
        return (f"currentRun: {self.currentRun}\n"
                f"prevRun: {self.prevRun}\n"
                f"certNumStart: {self.certNumStart}\n"
                f"certNumStop: {self.certNumStop}\n"
                f"maxCerts: {self.maxCerts}\n"
                f"qtrYear: {self.qtrYear}\n"
                f"qtr: {self.qtr}\n")


def increment_bank_params(run_params: DataRunParams):
    print("Proceeding to next 10000 bank charter numbers...")
    run_params.certNumStart += 10000
    run_params.certNumStop += 10000


def increment_cred_params():
    if DataRunParams.qtr == 4:
        DataRunParams.qtr = 1
        DataRunParams.qtrYear += 1
    else:
        DataRunParams.qtr += 1


def print_data_run_params_class():
    return print(f"currentRun: {DataRunParams.currentRun}\n"
                 f"prevRun: {DataRunParams.prevRun}\n"
                 f"certNumStart: {DataRunParams.certNumStart}\n"
                 f"certNumStop: {DataRunParams.certNumStop}\n"
                 f"maxCerts: {DataRunParams.maxCerts}\n"
                 f"qtrYear: {DataRunParams.qtrYear}\n"
                 f"qtr: {DataRunParams.qtr}\n")


def dev_reset_params():
    DataRunParams.currentRun = currentDate
    DataRunParams.prevRun = "2023-06-30"
    DataRunParams.certNumStart = 0
    DataRunParams.certNumStop = 9999
    DataRunParams.qtrYear = 2023
    DataRunParams.qtr = 1
