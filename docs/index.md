# Welcome to the Pipeline Docs
This pipeline collects data from the Federal Deposit Insurance Corporation (FDIC) and
National Credit Union Association (NCUA). The pipeline unpacks, validates, standardizes,
and organizes the data before writing it, table-ready and optimized, to Amazon S3 for querying through
Amazon Athena and Glue Data Catalog.

## Set Up Your Environment
See [Setup on Windows OS](setup-windows.md) for instructions.

## Implementing the Pipeline
From the command line, in the pipeline project's top level directory, enter:

    python pipeline1.py

which will run

    BuildBronzeLayer.update_bronze_layer()
    BuildSilverLayer.update_silver_layer()
    BuildGoldLayer.update_gold_layer()

## Project Layout

    docs/                           # Folder for documentation.
        bronze-ref.md                   # Bronze layer code documentation.
        gold-ref.md                     # Gold layer code documentation.
        helpers-ref.md                  # Helper functions and dicts documentation.
        index.md                        # Application docs homepage.
        setup-windows.md                # Instructions to set up the python environment.
        silver-ref.md                   # Silver layer code documentation.
    pipelineApplication/            # Folder of code definitions for job run of pipeline.
        bronzeLayer/                    # Folder of code for constructing the pipeline's Bronze layer.
            BankData.py                     # Code to query for bank information from the FDIC API.
            BuildBronzeLayer.py             # Code that implements the building of the Bronze layer data in S3.
            CreditUnionData.py              # Code to download and extract NCUA credit union data.
            DataRunParams.py                # Code managing the parameters for each run of the data pipeline.
        goldLayer/                      # Folder of code for constructing the pipeline's Gold layer.
            BuildGoldLayer.py               # Code that implements the building of the Gold layer data in S3.
        silverLayer/                    # Folder of code for constructing the pipeline's Silver layer.
            BuildSilverLayer.py             # Code that implements the building of the Silver layer data in S3.
            StateAbbreviationDict.py        # A single dictionary mapping state names to their abbreviations.
        Helpers_FunctionsDicts.py           # Code for helper functions and dictionaries.
    sparkLogs/                      # Folder of run logs produced by Spark Session.
    tests/                          # Folder of tests.
        test_Pyspark.py                 #  
    mkdocs.yml                      # The documentation configuration file.
    pipeline1.py                    # Data pipeline script.
    requirements.txt                # Lists package dependencies.
    runLog.txt                      # Log listing dates of previous runs from least to most recent.