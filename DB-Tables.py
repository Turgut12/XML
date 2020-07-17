# -*- coding: utf-8 -*-
"""
Created on Wed Jul 15 14:34:44 2020

@author: VUB
"""

from lxml import etree
import DataFrame
from psycopg2 import sql
import psycopg2
import sys
import table_schema



table_sizes = table_schema.table_sizes_dictionary
                 
tree = etree.parse("xml_files/test_2_b.xml") #load filed
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
    cursor.execute(table_schema.test_1_tbl_command)
    cursor.execute(table_schema.test_2_tbl_command)
    
    
#### uncommenting the for expression below would create all the tables within the DataBase
#    for command in table_schema.all_tbl_commands:
#        cursor.execute(command)
        
def add_attributes(table_name, lst_of_attributes):
    cursor.execute("SET DateStyle to European")
    try:
        in_str = ','.join(['%s'] * len(lst_of_attributes)) # This will generate a string '%s,%s,%s...', so that the attributes can be passed
        query = "INSERT INTO {} VALUES ({})".format(table_name, in_str)
        cursor.execute(query, lst_of_attributes)
    except Exception:
        print("ERROR: ILLEGAL VALUE - Users Table")
        print(sys.exc_info()[1])
        
def add_DataFrame(df, root):
    table_name = df[0][0]
    if table_name == "user":
        table_name = "users"
    record_fields = df[0].tolist()
    record_references = df[1].tolist()
    record_values = df[2].tolist()
    add_record(table_name, record_fields, record_references, record_values)

def reference_ID_and_subroot(file_path):
    sub_tree = etree.parse(file_path)
    sub_root = sub_tree.getroot()
    if sub_root == "user":
        sub_root = "users"
    df = DataFrame.objectify_root(sub_root)
    ID = df[2][1]
    return [ID, sub_root]

def reference_exists(file_path):
    ID_and_subroot = reference_ID_and_subroot(file_path)
    ID = ID_and_subroot[0]
    sub_root = ID_and_subroot[1]
    cursor.execute(sql.SQL("select exists(select 1 from {} where ID = (%s))").format(sql.Identifier(sub_root.tag)), [ID])
    boolean = cursor.fetchall()[0][0] #fetchall returns a list of tuples. We have to dereference twice to get the boolean
    return boolean
     
def add_record(table_name, record_fields, record_references, record_values):  
    def get_list_of_attributes():
        lst_of_attributes = []
        #indx = 0 (IGNORE INDX VAR FOR NOW)
        for field, attribute, value in zip(record_fields, record_references, record_values):
            #indx = indx + 1
            
            if value:
                lst_of_attributes.append(value)
                if (len(lst_of_attributes) == table_sizes[table_name]):
                    return lst_of_attributes
                
            elif "refid" in attribute:
                file_path = attribute["refid"]
                if (not reference_exists(file_path)): #check if the reference already exists in table
                      sub_tree = etree.parse(file_path)
                      sub_tree.xinclude()
                      sub_root = sub_tree.getroot()
                      df = DataFrame.objectify_root(sub_root)
                      add_DataFrame(df, sub_root) #if so, add the data within the file being referred to
                lst_of_attributes.append(reference_ID_and_subroot(file_path)[0]) #add the ID
                if (len(lst_of_attributes) == table_sizes[table_name]):
                    return lst_of_attributes
                
#            elif "fileName" in attribute: #branch for Xincluded records
#                add_record(field, record_fields[indx:], record_references[indx:], record_values[indx:]) #add the Xincluded record
#                attribute_to_skip = table_sizes[record_fields[0]]
#                while(attribute_to_skip): #the recursive call will take care of adding the Xincluded record, the parent has to skip the attributes
#                    print("skipping attributes")
#                    next(triplets)
#                    indx = indx + 1
#                    attribute_to_skip = attribute_to_skip - 1          
                    
    l = get_list_of_attributes()
    add_attributes(table_name, l)
    return True
        
        
        
create_tables()
add_DataFrame(df, root)

print('\n')
print("TEST_1 contents!!!")
cursor.execute("SELECT * FROM test_1")
print(cursor.fetchall())
print('\n')
print("TEST_2 contents!!!")
cursor.execute("SELECT * FROM test_2")
print(cursor.fetchall())

#cursor.execute("DROP TABLE test_1 CASCADE")
#cursor.execute("DROP TABLE test_2 CASCADE")
connection.commit()
    


#closing database connection.
if(connection):
    cursor.close()
    connection.close()
    print("PostgreSQL connection is closed")