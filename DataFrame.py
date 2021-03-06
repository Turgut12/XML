# -*- coding: utf-8 -*-
"""
Created on Wed Jul 15 14:34:03 2020

@author: VUB
"""
import pandas as pd

def objectify_file_to_list(root):
    """this function convert XML-file into a list of lists: [[child.tag, child.attrib, child.text], ...]"""
    data = [[root.tag, [], None]] #add root-tag
    def obj_part(part):
        for child in part:
            if (child.getchildren()): # look if the child refers to another file
                data.append([child.tag, child.attrib, child.text]) #if so, add the child-tag
                obj_part(child)       # and objectify that file aswell
            else: data.append([child.tag, child.attrib, child.text])
    obj_part(root)
    return data

def objectify_root(root):
    """this function uses the function objectify_file_to_list to make a DataFrame"""
    df = pd.DataFrame(
            [lst[0], lst[1], lst[2]] for lst in objectify_file_to_list(root)
            ) 
    for x in range(df[2].size):
        if (type(df[2][x]) == str and (df[2][x].startswith('\n') or df[2][x].startswith(' ') or df[2][x] == "")): #remove the useless strings
                df[2][x] = None 
    return df