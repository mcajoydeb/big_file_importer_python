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
        self.__data_file_name    = 'test_table_data.txt'
        self.__delimiter         = '|'
        self.__newline           = '\n'
        self.__header            = True
        self.__mysql_host        = "localhost"
        self.__mysql_port        = "3306"
        self.__mysql_db          = "venaudit"
        self.__mysql_table       = "cars_listing"
        self.__mysql_user        = "root"
        self.__mysql_password    = ""
        self.__db_connection     = self.__get_db_connection()
        self.__column_names      = self.__get_column_names()
        print("Connection successfull")
        self.__create_table_schema()
        print( "Table "+self.__mysql_table + " created successfull")
        self.__load_data_file_to_db()
        print("Data imported")
        
    
    def __get_column_names(self):

        data_file           = open(self.__data_file_name,"r")
        self.__header       = data_file.readline()
        data_file.close()

        column_names        = self.__header.split(self.__delimiter)
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
    
    def __load_data_file_to_db(self):

        print("Started importing data")

        load_statement = "LOAD DATA LOCAL INFILE '" + self.__data_file_name + "'\nINTO TABLE " + self.__mysql_db + "." + self.__mysql_table + "\nFIELDS TERMINATED BY '" + self.__delimiter + "'" + "\nLINES TERMINATED BY '" + self.__newline + "'" + "\nIGNORE 1 LINES\n("
        for field in self.__column_names:
            load_statement += field.replace("\n","") + ","
        load_statement = load_statement[:-1] + ");"
        
        cursor = self.__db_connection.cursor()
        cursor.execute(load_statement)
        self.__db_connection.commit()
        self.__db_connection.close()
        print("Data import in progress")

    def __create_table_schema(self):
        columns = ['id int NOT NULL AUTO_INCREMENT',
            'vin varchar(251) DEFAULT NULL',
            'year int NOT NULL',
            'make varchar(51) NOT NULL',
            'model varchar(51) DEFAULT NULL',
            'trim varchar(25) DEFAULT NULL',
            'dealer_name varchar(101) DEFAULT NULL',
            'dealer_street varchar(101) DEFAULT NULL',
            'dealer_city varchar(101) DEFAULT NULL',
            'dealer_state varchar(51) DEFAULT NULL',
            'dealer_zip varchar(15) DEFAULT NULL',
            'listing_price float DEFAULT NULL',
            'listing_mileage int DEFAULT NULL',
            'used int NOT NULL DEFAULT "0"',
            'certified int NOT NULL DEFAULT "0"',
            'style varchar(51) DEFAULT NULL',
            'driven_wheels varchar(11) DEFAULT NULL',
            'engine varchar(11) DEFAULT NULL',
            'fuel_type varchar(11) DEFAULT NULL',
            'exterior_color varchar(25) DEFAULT NULL',
            'interior_color varchar(25) DEFAULT NULL',
            'seller_website varchar(101) DEFAULT NULL',
            'first_seen_date date DEFAULT NULL',
            'last_seen_date date DEFAULT NULL',
            'dealer_vdp_last_seen_date date DEFAULT NULL',
            'listing_status int NOT NULL DEFAULT "0"',
            'is_updated int NOT NULL DEFAULT "0"',
            'created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP'
           ]
        
        cursor                          = self.__db_connection.cursor()
        create_table_schema_sql         = 'CREATE TABLE IF NOT EXISTS '+self.__mysql_table+' (' + ', '.join(columns) + ', PRIMARY KEY (`id`),KEY make (`make`), KEY model (`model`) ) ENGINE=InnoDB DEFAULT CHARSET=latin1';               
        cursor.execute(create_table_schema_sql)       
   
ImportData()
 

