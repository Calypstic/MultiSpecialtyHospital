# -*- coding: utf-8 -*-
"""
Created on Thu Nov 26 20:08:47 2020

@author: calypso
"""

import sqlite3

#this creates the database and the table, and puts data into table, has to run only one.
#This is data creation phase

def create_database():

    connection=sqlite3.connect(r'..\input\MultiSpecialtyHospital.db')
    crsr=connection.cursor()
    
    sql_command="""CREATE TABLE if not exists record (
    Customer_Name VARCHAR (255) NOT NULL PRIMARY KEY,
    Customer_Id VARCHAR (18) NOT NULL,
    Open_Date DATE (8) NOT NULL,
    Last_Consulted_Date  DATE(8),
    Vaccination_Id  CHAR(5),
    Dr_Name CHAR (255),
    State CHAR (5),
    Country CHAR (5),
    DOB DATE (8),
    Is_Active CHAR (1)
    );"""
    
    crsr.execute(sql_command)
    
    sql_command_insert=["""INSERT INTO  record 
    VALUES("Alex",123457,"2010-10-12","2012-10-13","MVD","Paul","SA","USA","06-03-1987","A");""","""INSERT INTO  record 
    VALUES("John",123458,"2010-10-12","2012-10-13","MVD","Paul","TN","IND","06-03-1987","A");""",
    """INSERT INTO  record 
    VALUES("Mathew",123459,"2010-10-12","2012-10-13","MVD","Paul","WAS","PHIL","06-03-1987","A");""",
    """INSERT INTO  record 
    VALUES("Matt",12345,"2010-10-12","2012-10-13","MVD","Paul","BOS","NYC","06-03-1987","A");""",
    """INSERT INTO  record 
    VALUES("Jacob",1256,"2010-10-12","2012-10-13","MVD","Paul","VIC","AU","06-03-1987","A");"""]
    
    for i in sql_command_insert:
        crsr.execute(i)
    connection.commit()
    
    crsr.close()
    connection.close()

create_database()