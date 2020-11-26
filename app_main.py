# -*- coding: utf-8 -*-
"""
Created on Thu Nov 26 15:47:14 2020

@author: calypso
"""

#importing libraries
import sqlite3
from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy import inspect
 
pd.options.mode.chained_assignment = None  # default='warn'



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



#data capture phase
#reading data into a dataframe and passing it into segmentation phase.
def read_data_into_dataframe():
    
    con=create_engine('sqlite:///..//input//MultiSpecialtyHospital.db').connect()
    df=pd.read_sql_table('record',con)
    con.close()
    #print(df)
    segment_database_into_parts(df)
    
    


#ETL phase
#This is the segmentation of the parent table into small tables based on countries
def segment_database_into_parts(df):
    #print(df.columns)
    
    
    #parsing strings to dates as python reads dates at strings when accepted as a dataframe
    df['Open_Date']=pd.to_datetime(df['Open_Date'])
    df['Last_Consulted_Date']=pd.to_datetime(df['Last_Consulted_Date'])
    df['DOB']=pd.to_datetime(df['DOB'])
 
    #print(df.describe(include='all'))   #this can describe attributes about data
   
    country_list=list(set(df['Country'].to_list()))
    #print(country_list)
    engine=create_engine('sqlite:///..//output//MultiSpecialtyHospitalUpdated.db',echo=True)
    con=engine.connect()
    
   
    for i in country_list:
        
        #segmentation 
        print(i)
        df1=df.loc[df['Country']==i]
        
        print(df1)
      
        #data warehousing phase
        #output to csv
        df1.to_csv(r'..\output\csv\Table_%s.csv'%i,index=False)
        #output to sql
        # sqlite_table = "Table_%s"%i
        # df1.to_sql(sqlite_table,con, if_exists='replace')
    
    con.close()
    #check_final_result(engine)    


#testing phase
def check_final_result(engine):
    inspector = inspect(engine)
    for table_name in inspector.get_table_names():
        for column in inspector.get_columns(table_name):
            print("Column: %s" % column['name'])
            
    con=engine.connect()
    df=pd.read_sql_table('Table_USA',con)
    print(df)
    con.close()




    
create_database()  #this has to be run only once
read_data_into_dataframe()
