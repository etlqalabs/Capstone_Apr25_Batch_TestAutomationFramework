# oracle data configuration
ORACLE_USER = 'system'
ORACLE_PASSWORD = 'admin'
ORACLE_HOST = 'localhost'
ORACLE_PORT = '1521'
ORACLE_DATABASE = 'xe'

# MYSQL data configuration
MYSQL_USER = 'root'
MYSQL_PASSWORD = 'Admin%40143'
MYSQL_HOST = 'localhost'
MYSQL_PORT = '3308'
MYSQL_DATABASE_STAGING = 'stag_retaildwg'
MYSQL_DATABASE_TARGET= 'retaildwh'


# Linux Setup SSH connection details
hostname = '192.168.0.111'  # Remote server's hostname or IP address
username = 'etlqalabs'  # SSH username
password = 'root'  # SSH password or use key-based authentication
remote_file_path = '/home/etlqalabs/sales_data.csv'  # Full path to the file on the remote server
local_file_path = 'TestData/sales_data_Linux_remote.csv'  # Local path to save the file
