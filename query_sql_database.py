import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
import oracledb 

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None) 
#Load credentials
load_dotenv()
user=os.getenv('user')
pw=os.getenv('password')
oserver1=os.getenv('oserver1')
oserver2=os.getenv('oserver2')

#Create connection to server
o_engine1 =create_engine(f"oracle+oracledb://{user}:{pw}@{oserver1}")
o_engine2 =create_engine(f"oracle+oracledb://{user}:{pw}@{oserver2}")

engines = [o_engine1, oengine2]
engine_names = {    oengine1: 'ENGEINE1',    oengine2: 'ENGINE2'} 

#Find Table or View
Search_key="'%psearch_text%'".lower()

#View All columns
#find table use dba_tables for all table in database, user all_tables for tables user has access, and use user_tables for tables the user own. Same apply to views
table_query = f"""    
    select *    
    from all_tables    
    where lower(table_name) like {Search_key} and status = 'VALID'    
    """
view_query = f"""    
    select *    
    from all_views    
    where owner<>'SYS' and lower(view_name) like {Search_key}    
    """

#View key columns
#find table use dba_tables for all table in database, user all_tables for tables user has access, and use user_tables for tables the user own. Same apply to views
table_query = f"""    
    select 'table' as source, owner, table_name as name    
    from all_tables    
    where lower(table_name) like {Search_key} and status = 'VALID'    
    """
view_query = f"""    
    select 'view' as source, owner, view_name as name, text as query    
    from all_views    
    where owner<>'SYS' and lower(view_name) like {Search_key}    
    """
df_database = pd.DataFrame() 
for engine in engines:    
  df_table = pd.read_sql(table_query, engine)    
  df_view = pd.read_sql(view_query, engine)    
  df=df_table._append(df_view)    
  df['Engine']= engine_names[engine]    
  df_database=df_database._append(df)   
  df_database 

#Find Column
Search_column="'%search_column%'".lower()
  
#find column
column_query = f"""    
    select owner, table_name, column_name, data_type, data_length    
    from sys.all_tab_columns    where lower(column_name) like {Search_column}    
    """
df_column = pd.DataFrame() 
for engine in engines:    
  df = pd.read_sql(column_query, engine)    
  df['Engine']= engine_names[engine]        
  df_column=df_column._append(df)df_column 

#Query Database
table="PROJECT_DAS_RFP_RPT_MV".lower()table 

#find data
sql_query = f"""    
    select *    
    FROM {table}    
    where rownum<10    
    """
df = pd.read_sql(sql_query, ords_engine)df df.columns.to_list()  

# Microsoft SQL
server_name = "SERVER_NAME"
database_name = "database_name"
fdw_engine = create_engine(f'mssql+pyodbc://{server_name}/{database_name}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server') 

#Find Table or View
ms_search_table="'%wo%'".lower() 
ms_table_query=f"""    
    SELECT TABLE_NAME    
    FROM INFORMATION_SCHEMA.TABLES    
    WHERE TABLE_NAME LIKE {ms_search_table}   
    """
df_ms_table= pd.read_sql(ms_table_query, fdw_engine)
df_ms_table 

#Find Column
ms_search_column="'%keyword%'".lower()
ms_column_query=f"""    
    SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, DATA_TYPE    
    FROM INFORMATION_SCHEMA.COLUMNS   
    WHERE COLUMN_NAME LIKE {ms_search_column}    
    """
df_ms_column= pd.read_sql(ms_column_query, fdw_engine)
df_ms_column 

#Query Database
ms_table="vw_DW_FusionAllPOs".lower()

#find data
ms_sql_query = f"""    
    select    TOP 10    *    
FROM dbo.{ms_table}    
"""
df_ms_query= pd.read_sql(ms_sql_query, fdw_engine)
df_ms_query
