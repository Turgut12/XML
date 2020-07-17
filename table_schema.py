# -*- coding: utf-8 -*-
"""
Created on Thu Jul 16 13:44:25 2020

@author: VUB
"""

###COMMANDS TO CREATE THE TABLES

test_1_tbl_command = ("""
                      CREATE TABLE test_1 (
                      ID UUID PRIMARY KEY,
                      WORD varchar,
                      NUMBER int
                      )
                      """
                        )

test_2_tbl_command = ("""
                      CREATE TABLE test_2 (
                      ID UUID PRIMARY KEY,
                      WORD varchar,
                      NUMBER int,
                      Test_ref UUID references test_1(ID)
                      )
                      """)

users_tbl_command = ("""
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
                   CreationDate date
                   )
                   """)
                   
                   
NC_FILE_tbl_command = ("""
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
                   
                   
CAM_tbl_command = ("""
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
                   
CAD_tbl_command = ("""
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
            
                   
Material_tbl_command = ("""
                   CREATE TABLE Material (
                   ID UUID PRIMARY KEY,
                   CreationDate varchar,
                   CreatedBy date,
                   Engineer varchar,
                   Operator UUID references users(ID),
                   Version varchar,
                   ParentVersion varchar,
                   Powder BOOL,
                   Category int,
                   TypeCode varchar,
                   Name varchar,
                   Supplier varchar
                   )
                   """)
                          
Part_tbl_command = ("""
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
                   
all_tbl_commands = [
                    ##tests
                    test_1_tbl_command,
                    test_2_tbl_command,
                    ##
        
                    users_tbl_command,
                    NC_FILE_tbl_command,
                    CAM_tbl_command,
                    CAD_tbl_command,
                    Material_tbl_command,
                    Part_tbl_command
                    #more tables can be added here
                       ]
        
table_names = ["test_1", "test_2", "users", "NC_file", "CAM", "CAD", "Material", "Part"]

def __make_table_size_dictionary():
    """makes a dictionary of the form dictionary of the form {"users": 10, "NC_file": 8, ...}"""
    name_command_dict = {
            table_name: table_command for table_name, table_command in zip(table_names, all_tbl_commands)
            } #dictionary of the form {"users": users_tbl_command, ...}
    
    def count_fields(table_creation_command):
        return table_creation_command.count(',') + 1 #count the number of commas in a table_creation_command, for example users_tbl_command
                                                     #we add one to this number because there is 1 more field than the number of commas
    table_sizes = {                                  
        table_name: count_fields(name_command_dict[table_name]) for table_name in table_names
        }
    return table_sizes

table_sizes_dictionary = __make_table_size_dictionary()
    

    



