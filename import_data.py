import mysql.connector as mysql
import mysql.connector

class ImportData():
    
 
    __data_file         = None
    __delimiter         = None
    __newline           = None
    __header            = None
    __mysql_host        = None
    __mysql_port        = None
    __mysql_db          = None
    __mysql_table       = None
    __mysql_user        = None
    __mysql_password    = None
    __column_names      = None
    __db_connection     = None           

    def __init__(self):
        self.__data_file_name    = 'vin_data.txt'
        self.__delimiter         = '|'
        self.__newline           = '\n'
        self.__header            = True
        self.__mysql_host        = "localhost"
        self.__mysql_port        = "3306"
        self.__mysql_db          = "venaudit"
        self.__mysql_table       = "text_file_import"
        self.__mysql_user        = "root"
        self.__mysql_password    = ""
        self.__db_connection     = self.__get_db_connection()
        self.__column_names      = self.__get_column_names()
        print("Connection successfull")
    
    def __get_column_names(self):

        data_file           = open(self.__data_file_name,"r")
        self.__header       = data_file.readline()
        data_file.close()

        column_names = self.__header.split(self.__delimiter)
        return column_names

    def __get_db_connection(self):
        
        db_connection = mysql.connector.connect(
                        host                = self.__mysql_host,
                        user                = self.__mysql_user,        
                        password            = self.__mysql_password,
                        database            = self.__mysql_db,
                        port                = self.__mysql_port,
                        allow_local_infile  = True
                )
        return db_connection
    
    def load_data_file_to_db(self):

        print("Started importing data")

        load_statement = "LOAD DATA LOCAL INFILE '" + self.__data_file_name + "'\nINTO TABLE " + self.__mysql_db + "." + self.__mysql_table + "\nFIELDS TERMINATED BY '" + self.__delimiter + "'" + "\nLINES TERMINATED BY '" + self.__newline + "'" + "\nIGNORE 1 LINES\n("
        for field in self.__column_names:
            load_statement += field.replace("\n","") + ","
        load_statement = load_statement[:-1] + ");"
        
        cursor = self.__db_connection.cursor()
        cursor.execute(load_statement)
        self.__db_connection.commit()
        self.__db_connection.close()
        print("Complete")
    
import_data = ImportData()
 
import_data.load_data_file_to_db()
