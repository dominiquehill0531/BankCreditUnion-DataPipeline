"""
Code managing the parameters for each run of the data pipeline.

Contains:
    runLog: list[str] - list of strings derived from text document log of previous run date strings.
    DataRunParams class definition and custom initialization logic.
"""

from datetime import datetime

from pipelineApplication.Helpers_FunctionsDicts import moNumQtrs

runLog = open("runLog.txt", "r")


class DataRunParams:
    """
    A class representing an object that holds and manages parameters for each run of data pipeline.

    Attributes:
        currentRun: String of current date in format '%Y-%m-%d'.
        prevRun: String of date of last job run in format '%Y-%m-%d'.
        qtrYear: Integer value of the year of the previous run.
        qtr: Integer value of the month of the previous run.
        certNumStart: Integer starting value for HTTP-requesting bank data by certification number.
        certNumStop: Integer ending value for HTTP-requesting bank data by certification number.
        maxCerts: Integer maximum value for HTTP-requesting bank data by certification number.
    """
    # Default changes with each run
    currentRun: str
    prevRun: str
    qtrYear: int
    qtr: int
    # Default remains
    certNumStart: int = 0
    certNumStop: int = 9999
    maxCerts: int = 100000  # No more than 100K Bank certs

    def __init__(self):
        """
        Constructs all dynamic attributes of the DataRunParams object without constant default values.
        """
        self.currentRun = datetime.today().strftime('%Y-%m-%d')
        self.prevRun = runLog.readlines()[-1]
        prev_run_date = datetime.strptime(self.prevRun, '%Y-%m-%d')
        self.qtrYear = int(prev_run_date.strftime('%Y'))
        self.qtr = moNumQtrs.get(int(prev_run_date.strftime('%m')))
        runLog.close()

    def to_str(self) -> str:
        """
        Prints string listing current values of DataRunParams object attributes for development purposes.

        Returns:
            String of attribute values.
        """
        return (f"currentRun: {self.currentRun}\n"
                f"prevRun: {self.prevRun}\n"
                f"certNumStart: {self.certNumStart}\n"
                f"certNumStop: {self.certNumStop}\n"
                f"maxCerts: {self.maxCerts}\n"
                f"qtrYear: {self.qtrYear}\n"
                f"qtr: {self.qtr}\n")

    def increment_bank_params(self):
        """
        Increments select attribute values needed for querying FDIC bank data API.
        """
        print("Proceeding to next 10000 bank charter numbers...")
        self.certNumStart += 10000
        self.certNumStop += 10000

    def increment_cred_params(self):
        """
        Increments select attribute values needed to request and process credit union data reports from the NCUA.
        """
        if self.qtr == 4:
            self.qtr = 1
            self.qtrYear += 1
        else:
            self.qtr += 1
