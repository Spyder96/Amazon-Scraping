# -*- coding: utf-8 -*-
"""
Created on Thu Apr 13 23:41:05 2023

@author: A
"""
import psycopg2 as pg
from psycopg2 import extras  as pgextras

class postgres_database:
    
    def __init__(self,dbname,user,password,host='localhost'):
        self.name = dbname
        self.user = user
        self.password = password
        self.host = host
        self.connection = None
        
    
    def connect(self):
        self.connection = pg.connect(
                dbname  = self.name,
                user    = self.user, 
                password= self.password,
                host    = self.host
            )
        self.connection.autocommit = True
        
    def create_database(self,name):
        if self.name == 'postgres':
            cursor = self.connection.cursor()
            # Execute the SQL query
            try:
                cursor.execute(f"CREATE Database {name}")
            except pg.errors.DuplicateDatabase:
                print(f"Database {name} Exists")
            except Exception as e:
                print (f'Error Creating Database {name}: {e}')
                self.connection.rollback()
            cursor.close()
        else:
            print("Need to connect to postgres database for this method")
            

# db = postgres_database(dbname="amazon", user="postgres", password="admin")
# db.connect()
# db.create_database('north')

    def create_table(self,table_name,columns):
        # 
        cursor = self.connection.cursor()
        try:
            create_table_query= " CREATE TABLE %s ( %s )   "
            cursor.execute(create_table_query,(pg.extensions.AsIs(self.table_name),','.join(columns)))
        except pg.errors.DuplicateTable:
            print("table exists")
        except Exception as e:
            print(f'Error creating table: {e}')
            
        cursor.close()
        #connection.close()
col= ['id bigserial Primary Key', '\nName varchar not null', '\nPrice real  ', '\nStars real ', '\nNumber_of_Ratings int', ' \nNumber_of_Answered_Questions int', ' \nAmazon_offerings text', ' \nBrief_Description text', '\nproduct_link text', '\nPage  int ', '\nDate timestamp']
# def table_inserts_df(name,df,connection):
#     tuples = [tuple(x) for x in df.to_numpy()]
#     cols=','.join(list(df.columns))
#     cursor = connection.cursor()
#     insert_query = """insert into %s(%s) values %%s  """ % (name,cols)
#     try:
#         pgextras.execute_values(cursor, insert_query, tuples)
#         connection.commit()
#     except(Exception, pg.DatabaseError) as error:
#         print("Error inserting values: %s" % error)
#         connection.rollback()
#         cursor.close()
#         return 1
#     print('values inserted to db')
#     cursor.close()
#     return 0

