import os
import os
from InvestmentFrame import GetFilesRemote
from InvestmentFrame import LoadDataDb
from InvestmentFrame import SetupConfig
from AppConfig import AppConfig as App
import logging


#Class AppConfig to store the fundamental confiurations. In production scenarios these can be stored as environment vbariables or part of orchestration software variables (for e.g. Airflow)

etl_config_file_path = App.config("etlconfigpath")

etl_config_file = App.config("etlconfigfilename")

etl_log_path = App.config("etllogpath")

data_filename_pattern = App.spasx300_config("spasx300_file_pattern") 

etl_config_file_abs = os.path.join(etl_config_file_path, etl_config_file)

data_attribute_filename = App.spasx300_config("spasx300_attrib_filename") 

attribute_file_abs = os.path.join(etl_config_file_path, data_attribute_filename)

#logging is captured in a file

logging.basicConfig(filename=os.path.join(etl_log_path,data_filename_pattern +".log"),
                    format='%(levelname)s %(asctime)s :: %(message)s',
                    level=logging.DEBUG)

try:
    #Setting up the required configuration for processing
    configsetup =  SetupConfig()

    get_local_config = configsetup.getconfig_details(etl_config_file_abs)

    data_attribute_details = configsetup.read_file_attribute(attribute_file_abs)

    #Getting the files from the remote server

    remote_files = GetFilesRemote(get_local_config,data_attribute_details)

    remote_files.get_files_from_server(data_filename_pattern)    
    
    #Load the file into panda df and then to the staging table
    
    load_to_db = LoadDataDb(get_local_config,data_attribute_details)

    load_to_db.load_to_df_staging_tbl()

    #From staging load to the target table

    load_to_db.load_to_target_tbl()

    #Finally move the files to arhive directory

    load_to_db.move_files_to_archive()

except:
    logging.critical('Error while processing the file ' + data_attribute_filename)
    raise
