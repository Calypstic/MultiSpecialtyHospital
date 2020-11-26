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


#data capture phase
#reading data into a dataframe and passing it into segmentation phase.
def read_data_into_dataframe():
    
    con=create_engine('sqlite:///..//input//MultiSpecialtyHospital.db').connect()
    df=pd.read_sql_table('record',con)
    con.close()
    
    segment_database_into_parts(df)
    


#ETL phase
#This is the segmentation of the parent table into small tables based on countries
def segment_database_into_parts(df):
    
    #parsing strings to dates as python reads dates at strings when accepted as a dataframe
    df['Open_Date']=pd.to_datetime(df['Open_Date'])
    df['Last_Consulted_Date']=pd.to_datetime(df['Last_Consulted_Date'])
    df['DOB']=pd.to_datetime(df['DOB'])
 
    #print(df.describe(include='all'))   #this can describe attributes about data
   
    country_list=list(set(df['Country'].to_list()))
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
        sqlite_table = "Table_%s"%i
        df1.to_sql(sqlite_table,con, if_exists='replace')
    
    con.close()
    check_final_result(engine)    


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





read_data_into_dataframe()
