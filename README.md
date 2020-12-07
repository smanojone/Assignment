# Assignment
The Project/Repository has the configuration and the src code components to extract the index source files from remote server and load it into a database for users to access



#src

The src folder has the source code required to perform the extract/transform/load process. Following are the python files 
    
    1. AppConfig - This python script has the class AppConfig, that contains the fundamental configuration details required for the processing of the files. In productions these                      can be implemented using environment variables or via an orchestration software (e.g. Airflow variables)
    
    2. InvestmentFrame - This is the framework that has been designed and developed to ensure that the 3 classes can be reused for processing of different files that needs to                              follow extract/load/transform pattern
    
            a).SetupConfig - This sets up all the configuration required for processing. The configuration is done via json files (present in config folder)  and the class                                      AppConfig
            
            b).GetFilesRemote - This supports implemenation of extracting the files from Remote server. Before extracting the files a check is done to ensure that the file is 
                                already not downloaded or not changed after the last download. Also, to check the integrity of the file, the line count is verified against the 
                                actual count of records.
            
            c). LoadDataDb - As the name suggests this class helps to implement the loading of the files to panda data frame, staging database table and finally to the target 
                             database table
                             
   3. get_sp_asx300.py - This python file implements the actual processing of the SP ASX index file using the framework(InvestmentFrame) and the configuration files.
   
   
 #Config
   
The config folder consist of the various configuration required for the project
   
   1. etlconfig - The etlconfig has various configuration details like remote server details, database server details etc. that can be used by multiple processes invovled in the                   ETL process
   2. SPASX300_NCS_CLS_attrib - This config file is specific to the source file that needs to be processed. It has details of the various fields, field/record separator, 
                                database table and procedure associated with the file
                                
#Database Objects: The objects responsible for managing the SP ASX 300 data in the relational database

  1. spasx300_staging.sql - The staging table that mirrors the source file and is truncate and load everytime there is execution of ETL process
  2. spasx300_detail.sql - This is the target table that persists data for all files and all days.
  3. usp_spasx300_load_to_target.sql - This user stored procedure is called from python script and is responsible for loading of data from staging(spasx300_staging) to target 
                                       table(spasx300_detail)
  4. spasx300_snapshot_view.sql - View created for the user and only has the snapshot for a script code for an effective day even though in the target table there might be 
                                  multiple rows in scenarios when for a day, the file has been updated more than once in source system
                                  
  
     Improvement Areas
     1. The panda dataframes were used to bring in  attribute level data quality checks, duplicate checks etc, but couldnt implement the data quality checks
     2. Removing the old files/data based on the compliance/legal/organisation requirement.
                                

    

