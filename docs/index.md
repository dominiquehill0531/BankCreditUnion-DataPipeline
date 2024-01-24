# Welcome to the Pipeline Docs
This pipeline collects data from the Federal Deposit Insurance Corporation (FDIC) and
National Credit Union Association (NCUA). The pipeline unpacks, validates, standardizes,
and organizes the data before writing it, table-ready and optimized, to Amazon S3 for querying through
Amazon Athena and Glue Data Catalog.

## Set Up Your Environment
See [Setup on Windows OS](setup-windows.md) for instructions.

## Implementing the Pipeline
From the command line, enter:

    python alpharank_pipeline1.py


## Project Layout

    docs/
        bronze-ref.md                   # Bronze layer code documentation
        gold-ref.md                     # Gold layer code documentation
        helpers-ref.md                  # Helper functions and dicts documentation.
        index.md                        # Application docs homepage.
        setup-windows.md                # Instructions to set up the python environment.
        silver-ref.md                   # Silver layer code documentation.
    pipelineApplication/
        bronzeLayer/                    # Folder of code for constructing the pipeline's Bronze layer.
            BankData.py
            BuildBronzeLayer.py         # Code that implements the building of the Bronze layer data in S3.
            CreditUnionData.py      
            DataRunParams.py
        goldLayer/                      # Folder of code for constructing the pipeline's Gold layer.
            BuildGoldLayer.py
        silverLayer/                    # Folder of code for constructing the pipeline's Silver layer.
            BuildSilverLayer.py
            StateAbbreviationDict.py
        Helpers_FunctionsDicts.py       # Code for helper functions and dictionaries.
    sparkLogs/
    tests/
        test_Pyspark.py
    mkdocs.yml                          # The documentation configuration file.
    pipeline1.py
    requirements.txt