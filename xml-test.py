# -*- coding: utf-8 -*-
"""
Created on Mon Jul 13 10:31:10 2020

@author: VUB
"""


# ATTEMPT 1:

#XML tests
#import xml.dom.minidom
#
#doc = xml.dom.minidom.parse("part_000001.xml")
#
#
#print(doc.nodeName)
#print(doc.firstChild.tagName)
#
#x = doc.getElementsByTagName("Version")
#
#print("%d CADS: " % x.length)
#for CAD in x:
#    print(CAD.getAttribute("name"))


#ATTEMPT 2:

#import lxml
#
#tree = lxml.etree.parse("part_000001.xml")
#pretty = lxml.etree.tostring(tree, encoding="unicode", pretty_print=True)
#print(pretty)

#import xml.etree.ElementTree as ET
#import xml.etree.ElementInclude as EI
#tree = ET.parse("part_000002.xml")
#root = tree.getroot()
#
#EI.include(root)


from lxml import etree

tree = etree.parse("part_000001.xml") #load file
tree.xinclude()                                                  #recursively includes files
#string = etree.tostring(tree, pretty_print=True).decode("utf-8") #decode will convert bytes into string
#print(string)

root = tree.getroot()


### root -> dictionary
def objectify_root(root):
    keys = []
    values = []
    def obj_part(part):
        for child in part:
            if (child.getchildren()): # look if the child refers to another file
                obj_part(child)       # if so, objectify that file aswell
            keys.append(child.tag)
            values.append(child.text)
    obj_part(root)
    return dict(zip(keys, values))


dictn = objectify_root(root)
print("accessing dictionary!")
print(dictn.get("ProductionDate"))
        

###root -> List
def objectify_roott(root):
    data = []
    def obj_part(part):
        for child in part:
            if (child.getchildren()): # look if the child refers to another file
                obj_part(child)       # if so, objectify that file aswell
            data.append([child.tag, child.attrib, child.text])
    obj_part(root)
    return data

data = objectify_roott(root)
    

















