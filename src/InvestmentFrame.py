import pysftp
import json
import os
import re
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from urllib.parse import quote_plus

class SetupConfig:

    def getconfig_details(self, configfile):
        """
        Method to get all the config details required for performing the job
     
        """
        
        try:
            with open(configfile, "r") as read_file:
                config_details = json.loads(read_file.read())
        except:
            raise Exception('Error while opening the config file')
        else:    
            return config_details
        #return json.loads(configfile)
    
    def read_file_attribute(self, file_attribute):
        """
        Method to read the  source file attributes that include datatype, length etc.
        
        """
        try:
            with open(file_attribute, "r") as read_file:
                file_attribute_details = json.loads(read_file.read())
        except:
            raise Exception('Error while getting the file attributes from json for processing')
        else:
            return file_attribute_details

class GetFilesRemote:
    
    #Constructor

    def __init__(self, get_local_config,data_attribute_details):
        self.get_local_config = get_local_config
        self.data_attribute_details = data_attribute_details

    @staticmethod
    def diff_between_systems(remote_files_abs, archive_file_abs, conn):
        """
        Method to compare the files between the remote server and the archive folder in the local system
        
        """

        if os.path.exists(archive_file_abs) and conn.lstat(remote_files_abs).st_size == os.stat(archive_file_abs).st_size and conn.lstat(remote_files_abs).st_mtime == os.stat(archive_file_abs).st_mtime:
            return False
        else:
            return True 
     
    
    
    def get_files_from_server(self,data_filename_pattern):
        """
        Method to connect to the rolling window server to get the files following a pattern
        
        """
       
        try:  
            conn = pysftp.Connection( host = self.get_local_config['hostname'], username = self.get_local_config['username'], private_key= self.get_local_config['key'] )
            for eachfile in conn.listdir(self.get_local_config['remotepath']):
                if re.search(data_filename_pattern, eachfile):
                    remote_file_abs = os.path.join(self.get_local_config['remotepath'], eachfile)
                    archive_file_abs = os.path.join(self.get_local_config['archivepath'], eachfile)
                    staging_file_abs =  os.path.join(self.get_local_config['stagingpath'], eachfile)
                    if self.diff_between_systems(remote_file_abs, archive_file_abs,conn):
                        conn.get(remote_file_abs, staging_file_abs, preserve_mtime=True)
        except:
            raise Exception('Error while getting the files from the remote server')
        
class LoadDataDb:        
    
    
    #Constructor

    def __init__(self, get_local_config,data_attribute_details):
        self.get_local_config = get_local_config
        self.data_attribute_details = data_attribute_details

        
    @staticmethod
    def check_file_integrity(self,filename, bottom,row_count):
        
        """
        Method to check the record count in the file with the trailer information record count and raise error if it doesn't match
        
        """
        try:
            tail_record_pattern = self.data_attribute_details['source_detail']['tail_record_count_pattern']
            
            if re.search(tail_record_pattern, bottom):
                count = 1
                if self.data_attribute_details['source_detail']['has_header']:
                    count +=1
                file_rec_count = bottom.split(self.data_attribute_details['source_detail']['tail_field_delimiter'])[1]
                if file_rec_count != row_count -count:
                    return False
        except:
            raise Exception('Error while checking the file integrity')    
        else:
            return True

    @staticmethod
    def create_con_engine(self):
        """
        Method to create the engine that can be used for all the db activity
        
        """
        try:
            db_user = self.get_local_config['db_user']
            db_hostname  = self.get_local_config['db_hostname']
        
            #In real production scenario the password will not be stored in a file, but will be secured in service like AWS Secrets Manager etc.
            db_password  = self.get_local_config['db_password']
            db_port =self.get_local_config['db_port']
            database_name = self.get_local_config['database_name']
        
            engine = create_engine("postgresql://" + db_user + ":" + db_password + "@" + db_hostname + ":" + db_port + "/" + database_name, execution_options={ 
                                                                                                                                            "autocommit": "True"                            
                                                                                                                                        }   
                    )
        
        except:
            raise Exception('Error while connecting to the db')
        else:
            return engine

    @staticmethod
    def write_df_to_db(self,df):
        """
        Method to write the data to staging table if the file integrity check has passed
        
        """
        try:
            engine = self.create_con_engine(self)
            #engine = create_engine('postgresql://db_user:db_password@db_hostname:5432/database_name')
        
            table_name = self.data_attribute_details['target_detail']['staging_table']
            #The dataframe is written to the staging table
            df.to_sql(table_name, engine, if_exists='append', index=False)
        except:
            raise Exception('Error while writing to data frame')
            
    
    def load_to_df_staging_tbl(self):
        """
        Method to read the remotely downloaded files into panda dataframes
        
        """    
        try:
            for filename in os.listdir(self.get_local_config['stagingpath']):
            
                with open(os.path.join(self.get_local_config['stagingpath'],filename), 'r') as f_read:
                    bottom = f_read.readlines()[-1]
                df = pd.read_csv(os.path.join(self.get_local_config['stagingpath'],filename),sep=self.data_attribute_details['source_detail']['delimiter'],lineterminator= self.data_attribute_details['source_detail']['line_terminator']  )
                #The row count also includes the trailer record. 
                row_count = df.shape[0]
            
                #Drop the trailer record after taking the count
                if self.data_attribute_details['source_detail']['tail_record_count_pattern']:
                    df = df[:-1] 
                
                #if self.check_file_integrity(filename, bottom,row_count):
                self.write_df_to_db(self,df)
        except:
            raise Exception('Error while writing to staging table')    

    def load_to_target_tbl(self):
        """
        Method to load the data from the staging table to the target table
        
        """ 
        try:
            engine = self.create_con_engine(self)
        
            target_table_proc = self.data_attribute_details['target_detail']['target_table_proc']
            connection = engine.connect()
                
            
            result = connection.execute("CALL " + target_table_proc)  
        except:
            raise Exception('Error while writing to the target table')
        
        finally:
            connection.close
    
    def move_files_to_archive(self):
        try:
            for filename in os.listdir(self.get_local_config['stagingpath']):
                stage_file_abs = os.path.join(self.get_local_config['stagingpath'],filename)
                archive_file_abs = os.path.join(self.get_local_config['archivepath'],filename)
                os.rename(stage_file_abs,archive_file_abs)
        except:
            raise Exception('Error while moving source files to archive')
