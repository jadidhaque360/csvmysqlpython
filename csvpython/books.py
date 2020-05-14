# program to insert multiple data within a table
import mysql.connector
import pandas as pd
import numpy
import os


#change your details in here
host = 'localhost'
port = 3306
user = 'root'
password = 'tiger'
database = 'student' # give an existing database name here



def close_connection(my_con, my_cursor):
    if my_con.is_connected():
        my_cursor.close()
        print('cursor object got closed')
        my_con.close()
        print('connection got closed from database')

def insert_multiple_records():
    try:
        my_con = mysql.connector.connect(host=host, port=port, user=user, password=password, database=database)
        print(f'python is now connected to mysql databse')
        my_cursor = my_con.cursor()
        table_list = []
        new_table_list = []


        path=os.path.dirname(os.path.abspath(__file__)) #to find current path
        for filename in os.listdir(path): # if we want to use current directory we can just write path inside() or custom directory then give directory inside ()
            if filename.endswith('.csv'):

                table_list.append(filename)  #creating list of all files in the directory
                new_table_list.append(filename[:-4])    #creating list of all files names in the directory to be used as table names
        y=0
        for j in range(3):
            data1 = pd.read_csv(table_list[y])
            tab = new_table_list[y]
            db=database

            col1 = str(data1.columns[0]) #extracting the column names from the csv files
            col2 = str(data1.columns[1])
            col3 = str(data1.columns[2])
            col4 = str(data1.columns[3])
            col5 = str(data1.columns[4])
            col6 = str(data1.columns[5])
            sql_create_table="""CREATE TABLE IF NOT EXISTS %s.%s (`%s` INT NOT NULL, `%s` VARCHAR(45) NULL, `%s` VARCHAR(45) NULL, `%s` VARCHAR(45) NULL, `%s` VARCHAR(45) NULL, `%s` VARCHAR(45) NULL, PRIMARY KEY (%s))""" % (db,tab, col1, col2, col3, col4, col5, col6,col1)

            my_cursor.execute(sql_create_table) #created the table

            x=0
            for i in range(5):
                row1 = data1.loc[x] #taking x so as to add all the rows one by one
                l = int(row1[0])
                m = str(row1[1])
                n = str(row1[2])
                p = str(row1[3])
                q = str(row1[4])
                r = str(row1[5])

                sql_insert_query = ("""insert into %s(`%s`,`%s`,`%s`,`%s`,`%s`,`%s`) values(%s,'%s','%s','%s','%s','%s')""" % (tab, col1, col2, col3, col4, col5, col6, l, m, n, p, q, r))
                my_cursor.execute(sql_insert_query) #data inserted into tables
                print(f'sql queries for table/csv {table_list[y]} and row {row1[0]} inserted in to tables')
                my_con.commit()
                x=x+1
            y=y+1
        print('changes has been updated within the database')
    except mysql.connector.Error as msg:
        print(f'the cause of the exception is :{msg}') #for catching of exceptions
    finally:
        close_connection(my_con, my_cursor)


if __name__ == '__main__':
    insert_multiple_records()

