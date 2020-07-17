# -*- coding: utf-8 -*-
"""
Created on Wed Jul 15 14:34:44 2020

@author: VUB
"""

from lxml import etree
import DataFrame
import psycopg2
import sys
import table_schema

table_sizes = table_schema.table_sizes_dictionary
                 
tree = etree.parse("part_000001.xml") #load file
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
    
    cursor.execute(table_schema.users_tbl_command)
    
#### uncommenting the for expression below would create all the tables within the DataBase
#    for command in table_schema.all_tbl_commands:
#        cursor.execute(command)
        
def add_attributes(table_name, lst_of_attributes):
    cursor.execute("SET DateStyle to European")
    try:
        in_str = ','.join(['%s'] * len(lst_of_attributes)) # This will generate a string '%s,%s,%s...', so that the attributes can be passed
        query = "INSERT INTO {} VALUES ({})".format(table_name, in_str)
        cursor.execute(query, lst_of_attributes)
        cursor.execute("SELECT * FROM users")
        print(cursor.fetchall())
    except Exception:
        print("ERROR: ILLEGAL VALUE - Users Table")
        print(sys.exc_info()[1])
        
def add_DataFrame(df):
    table_name = root.tag
    if table_name == "user":
        table_name = "users"
    record_fields = df[0].tolist()
    record_references = df[1].tolist()
    record_values = df[2].tolist()
    add_record(table_name, record_fields, record_references, record_values)

def reference_ID_and_subroot(reference):
    sub_tree = etree.parse(reference)
    sub_root = sub_tree.getroot()
    if sub_root == "user":
        sub_root = "users"
    df = DataFrame.objectify_root(sub_root)
    ID = df[2][0]
    return [ID, sub_root]

def reference_exists(reference):
    ID_and_subroot = reference_ID_and_subroot(reference)
    ID = ID_and_subroot[0]
    sub_root = ID_and_subroot[1]
    return cursor.execute("select exists(select 1 from %s where ID = %s)", [sub_root.tag, ID])
     
def add_record(table_name, record_fields, record_references, record_values):  
    def get_list_of_attributes():
        lst_of_attributes = []
        indx = 0
        triplets = zip(record_fields, record_references, record_values)
        for field, reference, value in triplets:
            indx = indx + 1
            
            if value == "Xin":#this branch handles Xincluded records, by recursively calling add_record
                add_record(field.split('_')[0], record_fields[indx:], record_references[indx:], record_values[indx:])
                attributes_to_skip = table_sizes[field.split('_')[0]]
                while(attributes_to_skip): #the recursive call will take care of adding the Xincluded record, the parent has to skip the attributes
                    next(triplets)
                    indx = indx + 1
                    attributes_to_skip = attributes_to_skip - 1
                    
            elif value:
                lst_of_attributes.append(value)
                if (len(lst_of_attributes) == table_sizes[table_name]):
                    return lst_of_attributes
                
            elif reference:
                if (not reference_exists(reference)): #check if the reference already exists in table
                      sub_tree = etree.parse(reference)
                      sub_tree.xinclude()
                      sub_root = sub_tree.getroot()
                      df = DataFrame.objectify_root(sub_root)
                      add_DataFrame(df) #if add the Data within the file being referred to
                lst_of_attributes.append(reference_ID_and_subroot(reference)[0]) #otherwise, add the ID
                    
    l = get_list_of_attributes()
    print(l)
    add_attributes(table_name, l)
    return True
        
        
        
#create_tables()
#add_DataFrame(df)
#cursor.execute("DROP TABLE Part CASCADE")
#cursor.execute("DROP TABLE users CASCADE")
connection.commit()
    


#closing database connection.
if(connection):
    cursor.close()
    connection.close()
    print("PostgreSQL connection is closed")