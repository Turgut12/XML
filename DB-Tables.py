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
from pprint import pprint #used for debugging


table_sizes = table_schema.table_sizes_dictionary
                 
file_path = "xml_files/test_2.xml"


###the next 4 lines could be removed, they're used to see the DataFrame formed with the root.
###DataFrames are formed within the procedure "add_file"
tree = etree.parse(file_path) #load file
tree.xinclude() #Xinclude other files
root = tree.getroot() 
df = DataFrame.objectify_root(root) #make DataFrame 
####

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
        cursor.execute(query, lst_of_attributes) #Executes the actual query in SQL
    except Exception:
        print("ERROR: ILLEGAL VALUE - Users Table")
        print(sys.exc_info()[1]) #Shows the error message
        
def add_file(file_path):
    
    tree = etree.parse(file_path) #load file
    tree.xinclude() #Xinclude other files
    root = tree.getroot() 
    df = DataFrame.objectify_root(root) #make DataFrame 
    
    
    table_name = df[0][0]
    if table_name == "user":
        table_name = "users"
    record_fields = df[0].tolist() #get list of fields
    record_references = df[1].tolist() #get list
    record_values = df[2].tolist() #get list of values
    return add_record(record_fields, record_references, record_values)

def reference_ID_and_subroot(file_path):
    sub_tree = etree.parse(file_path)
    sub_root = sub_tree.getroot()
    if sub_root == "user": #the table name "user" is Illegal, and so we have to use the name "users" instead
        sub_root = "users"
    df = DataFrame.objectify_root(sub_root)
    ID = df[2][1] #the index of the ID within the DataFrame is [2][1]
    return [ID, sub_root]

def reference_exists(file_path):
    ID_and_subroot = reference_ID_and_subroot(file_path)
    ID = ID_and_subroot[0]
    sub_root = ID_and_subroot[1]
    cursor.execute(sql.SQL("select exists(select 1 from {} where ID = (%s))").format(sql.Identifier(sub_root.tag)), [ID])
    boolean = cursor.fetchall()[0][0] #fetchall returns a list of tuples. We have to dereference twice to get the boolean
    return boolean

def add_record(record_fields, record_references, record_values):
    all_records = preprocess_DataFrame(record_fields, record_references, record_values)
    
    def get_attributes(record):
        record.pop(0) #the first element contains the table name, and no attributes. Therefore it must be popped
        lst_of_attributes = []
        for field, attribute, value in record:
                lst_of_attributes.append(value)
        return lst_of_attributes
                
    for record in all_records:
        table_name = record[0][0]
        attributes = get_attributes(record)
        add_attributes(table_name, attributes)
    return True
                
            
        
def preprocess_DataFrame(record_fields, record_references, record_values):
    print("starting")
    all_records = []
    def look_for_record():
        nmbr_of_found_attributes = 0
        current_field_tag = record_fields[0]
        indx = 0
        current_record = []
        for field, attribute, value in zip(record_fields, record_references, record_values):
            indx = indx + 1
            if field in table_schema.table_names: #does the field refer to a new record?
                current_field_tag = field #if so, change the field_tag
                nmbr_of_found_attributes = 0  #we start looking for the attributes of the record, and thus have to start over at 0
                current_record = [[current_field_tag, [], None]] #initialize the record
                
            elif "refid" in attribute or value: #check if we found an attribute
                nmbr_of_found_attributes = nmbr_of_found_attributes + 1
                if "refid" in attribute:
                    file_path = attribute["refid"]
                    if (not reference_exists(file_path)): #check if the file being refered to is already in the database
                        add_file(file_path) #add file recursively
                        
                if "refid" in attribute:
                      current_record.append([field, [], reference_ID_and_subroot(file_path)[0]]) #add foreign key to record
                else: current_record.append([field, attribute, value]) #otherwise, add value to record
                
                if (nmbr_of_found_attributes == table_sizes[current_field_tag]): #check if we found all attributes of a record
                    
                    #the body of the if-test will cut the found record from record_fields, record_references and record_values
                    #and return the found record, so that it can be added to the database
                    first_attribute_indx = indx - table_sizes[current_field_tag] - 1 
                    del record_fields [first_attribute_indx:indx] #delete from the index of first attribute, to the index of last attribute
                    del record_references [first_attribute_indx:indx]
                    del record_values [first_attribute_indx:indx]
                    return current_record
        return False
                
    while (record_fields): 
        record = look_for_record() #we keep looking for records and appending them, until record_fields are empty
        if record:
            all_records.append(record)
        else: break #false was returned out of look_for_record()

    #pprint(all_records) #uncomment this line to pretty print all_records
    return all_records

                    
        
        
        
create_tables()
add_file("xml_files/test_2.xml")

print('\n')
print("TEST_1 contents!!!")
cursor.execute("SELECT * FROM test_1")
pprint(cursor.fetchall())
print('\n')
print("TEST_2 contents!!!")
cursor.execute("SELECT * FROM test_2")
pprint(cursor.fetchall())

cursor.execute("DROP TABLE test_1 CASCADE")
cursor.execute("DROP TABLE test_2 CASCADE")
    

connection.commit()
    


#closing database connection.
if(connection):
    cursor.close()
    connection.close()
    print("PostgreSQL connection is closed")