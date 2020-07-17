# -*- coding: utf-8 -*-
"""
Created on Wed Jul 15 14:34:44 2020

@author: VUB
"""

from lxml import etree
import DataFrame
import psycopg2
import sys



tree = etree.parse("test_user.xml") #load file
tree.xinclude()   

root = tree.getroot()

df = DataFrame.objectify_root(root)

connection = psycopg2.connect(user = "student-user",
                              password = "MiCLAD2020",
                              host = "192.168.142.4",
                              port = "5432",
                              database = "student-user")

cursor = connection.cursor()
# Print PostgreSQL Connection properties
print ( connection.get_dsn_parameters(),"\n")

# Print PostgreSQL version
cursor.execute("SELECT version();")
record = cursor.fetchone()
print("You are connected to - ", record,"\n")

def create_tables():   
    ###### TABLE 'Part' CANNOT YET BE CREATED BECAUSE IT DEPENDS ON OTHER TABLES
    
    cursor.execute("""
                   CREATE TABLE users (
                   ID UUID PRIMARY KEY,
                   Username varchar,
                   UserType int,
                   DBRights int,
                   MachineRights int,
                   DAQRights int,
                   AppRights int,
                   Password varchar,
                   Email varchar,
                   PhoneNumber varchar,
                   CreationDate date
                   )
                   """)
                   
    cursor.execute("""
                   CREATE TABLE NC_FILE (
                   ID UUID PRIMARY KEY,
                   Path varchar,
                   Date date,
                   Version varchar,
                   Engineer UUID references users(ID),
                   Parent INT,
                   Properties JSONB,
                   Lines int
                   )
                   """
                   )
                   
                   
    cursor.execute("""
                   CREATE TABLE CAM (
                   ID UUID PRIMARY KEY,
                   Path varchar,
                   Date date,
                   Version varchar,
                   Engineer UUID references users(ID),
                   Parent INT,
                   Properties JSONB
                   )
                  """)
                   
    cursor.execute("""
                   CREATE TABLE CAD (
                   ID UUID PRIMARY KEY,
                   WorkingPath varchar,
                   Date date,
                   Version varchar,
                   Engineer UUID references users(ID),
                   Parent INT,
                   Properties JSONB
                   )
                   """)
            
                   
#    cursor.execute("""
#                   CREATE TABLE Material (
#                   ID UUID PRIMARY KEY,
#                   CreationDate varchar,
#                   CreatedBy date,
#                   Engineer varchar,
#                   Operator UUID references users(ID),
#                   Version varchar,
#                   ParentVersion varchar,
#                   Powder BOOL,
#                   Category int,
#                   TypeCode varchar,
#                   Name varchar,
#                   Supplier varchar
#                   )
#                   """)
                          
    cursor.execute("""
                   CREATE TABLE Part (
                   ID UUID PRIMARY KEY,
                   Status varchar,
                   CAD_ID UUID references CAD(ID),
                   CAM_ID UUID references CAM(ID),
                   NC_ID UUID references NC_file(ID),
                   Engineer UUID references users(ID),
                   Designer UUID references users(ID),
                   Operator UUID references users(ID),
                   Project varchar,
                   CAMEngineer UUID references users(ID)
                   )
                   """)
                   
def add_record(table, lst_of_attributes):
    
    def sql_command(): ## this function generates the appropriate string for the INSERT SQL operation
        sqlstring = f"INSERT INTO {table} VALUES("

        for val in lst_of_attributes:
            sqlstring += f"'{val}'," 
  
        sqlstring = sqlstring[:-1] #remove the last comma
        sqlstring += ")"
        return sqlstring
    
    cursor.execute("SET DateStyle TO European")
    try:
        cursor.execute(sql_command().format(table, lst_of_attributes))
        cursor.execute("SELECT * FROM users")
        print(cursor.fetchall())
    except Exception:
        print("ERROR: ILLEGAL VALUE - User Table")
        print(sys.exc_info()[1])
        


test_l = df[2].values.tolist()

add_record("users" , test_l)
    
                   
#def add_user():
#    """to be written"""
               
        
#
#create_tables()
#cursor.execute("DROP TABLE Part CASCADE")
#cursor.execute("DROP TABLE users")
connection.commit()
    


#closing database connection.
if(connection):
    cursor.close()
    connection.close()
    print("PostgreSQL connection is closed")