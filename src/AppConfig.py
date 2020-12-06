class AppConfig:
   conf = {
    "etlconfigpath": "C:\\Project\\Config\\",
    "etlconfigfilename":"etlconfig.json",
    "etllogpath":"C:\\Project\\logs\\"
    
      }
   spasx300_conf = {
   "spasx300_file_pattern":"SPASX300_NCS_CLS",
   "spasx300_attrib_filename":"SPASX300_NCS_CLS_attrib.json"
      }

   @staticmethod
   def config(name):
    return AppConfig.conf[name]

   @staticmethod
   def spasx300_config(name):
    return AppConfig.spasx300_conf[name]
