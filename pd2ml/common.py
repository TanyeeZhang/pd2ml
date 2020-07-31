# coding=utf-8
"""common.py - This file defines some constants.
"""

UPLOAD_DIR_PREFIX = '__tmp__to__'

DOWNLOAD_DIR_PREFIX = '__tmp_from__'

UPLOAD_MODE = 'REPLACE'

LOAD_FILE_SUFFIX = '.csv'

COLUMNS_FILENAME = 'str_columns'

TYPE_MAPPING = {
    'int8': ('tinyint', ),
    'int16': ('int', 'smallint', ),
    'int32': ('mediumint', 'int'),
    'int64': ('bigint', ),
    'float64': ('float', 'double', 'decimal', 'real', 'numeric'),
    'timedeleta64[ns]': ('time', ),
    'datetime64[ns]': ('datetime', 'timestamp')
}
# TODO:
# SET autocommit = 0;
# SET unique_checks = 0;
# SET foreign_key_checks = 0;
# SET sql_log_bin=0;
# COMMIT;
UPLOAD_COMMAND = """LOAD DATA LOCAL INFILE '{}' {} INTO TABLE {} FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' 
                    LINES TERMINATED BY '{}' {} SET {};"""

DOWNLOAD_COMMAND = """SELECT {} INTO OUTFILE '{}' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
                      LINES TERMINATED BY '{}' FROM {};"""

COLUMNS_COMMAND = """SHOW COLUMNS FROM {}"""