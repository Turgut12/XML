# -*- coding: utf-8 -*-
"""
Created on Tue Jul 14 14:42:07 2020

@author: VUB
"""

from lxml import etree
import pandas as pd
import psycopg2

tree = etree.parse("part_000001.xml") #load file
tree.xinclude()   

root = tree.getroot()

###validate datetime object
#def isdatetime(string):
#    try:
#        datetime.datetime.strptime(string, '%d-%m-%Y)
#    except: False
#        raise False


###root -> List
def objectify_file_to_list(root):
    """this function convert XML-file into a list of lists: [[child.tag, child.attrib, child.text], ...]"""
    data = []
    def obj_part(part):
        for child in part:
            if (child.getchildren()): # look if the child refers to another file
                obj_part(child)       # if so, objectify that file aswell
            data.append([child.tag, child.attrib, child.text])
    obj_part(root)
    return data

###root -> DataFrame, using the procedure above
def objectify_root(root):
    """this function uses the function objectify_file_to_list to make a DataFrame"""
    df = pd.DataFrame(
            [lst[0], lst[1], lst[2]] for lst in objectify_file_to_list(root)
            ) 
    for x in range(df[2].size):
        if (type(df[2][x]) == str and df[2][x].startswith('\n')): #remove the useless strings
                df[2][x] = None 
    return df

df = objectify_root(root)

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
#    cursor.execute("""
#            CREATE TABLE Part (
#                ID UUID PRIMARY KEY,
#                Status varchar,
#                CAD_ID UUID references CAD(ID),
#                CAM_ID UUID references CAN(ID),
#                NC_ID UUID references NC_file(ID),
#                Engineer UUID references users(ID),
#                Designer UUID references users(ID),
#                Operator UUID references users(ID),
#                Project varchar,
#                CAMEngineer UUID references users(ID)
#            )
#            """)
    
    cursor.execute("""
                   CREATE TABLE users (
                   ID UUID PRIMARY KEY,
                   Username varchar,
                   UserType int,
                   DBRights int,
                   MachineRights int,
                   DAQRights int,
                   AppRights int,
                   Password int,
                   Email varchar,
                   PhoneNumber varchar,
                   CreationDate date
                   )
                   """)
            
            
create_tables()
#cursor.execute("DROP TABLE Part")
connection.commit()
    


#closing database connection.
if(connection):
    cursor.close()
    connection.close()
    print("PostgreSQL connection is closed")