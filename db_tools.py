import cx_Oracle
from sqlalchemy.engine import create_engine
import pandas as pd

class Db_tools:

    @staticmethod
    def clob_destroyer(clob_list:list) -> list:
    
        '''This function converts a list or array of clobs to their string values
        The function is designed for an oracle database, and requires cx_Oracle.read()
        
        '''        
        out_list = []        
        for clob in clob_list:        
            c_text = clob.read()            
            out_list.append(c_text)            
        return out_list

    @staticmethod
    def construct_table(df:object, name_string:str, dtype_dict:dict, p_key:str, fk_list:list, ref_list:list, eng:object) -> str:
        
        '''
        This function inserts a df into a database, and automatically 

        This is a function that relies on sql alchemy to create a table in an oracle database
        eng is a sql alchemy engine object

        function uses Oracle SQL

        '''
        
        #make table with pd.df.to_sql()
        df.to_sql(name_string, eng, if_exists = 'replace', index = False, dtype = dtype_dict)
        
        #set primary key
        pk_query = f'ALTER TABLE {name_string} ADD PRIMARY KEY ({p_key})'
        
        eng.execute(pk_query)
        
        #set foreign keys
        if fk_list == True:
        
            for k, r in zip(fk_list, ref_list):
            
                fk_query = f'ALTER TABLE {name_string} ADD FOREIGN KEY ({k}) REFERENCES {r}({k})'
            
                eng.execute(fk_query)
            
        return f'{name_string} table created'

    @staticmethod
    def wipe_db(table_list:list, eng:object) -> str:
        
        '''
        Wipe a full database using an sql alchemy engine
        
        For this to work you need to pass a list, that is correctly ordered in a way that the table can be dropped
        A table cannot be dropped if it contains foreign keys that reference a primary key in another table; that 
        table with the primary key must be dropped first. Drop the central tables first, then the ones at the periphery

        function uses Oracle SQL
        '''
        
        for table in table_list:
            
            drop_query = f'DROP TABLE {table}'
            
            try:
                eng.execute(drop_query)
                print(f'query {drop_query} executed')
                
            except:
                print(f'query {drop_query} not executed due to error')
                
        return 'Database Wipe Executed'

    @staticmethod
    def memory_profile(df:object, key_list:list) -> tuple:
    
        '''
        This function calculates the memory usage of a pandas dataframe
        If you pass it a dataframe and a list of the keys in the dataframe this will return a tuple
        First is the data usage in mb, second is data usage in bytes
        '''
    
        mem_use = 0
    
        for key in key_list:
            mem_comp = df[key].memory_usage(deep = True)
        
            mem_use += mem_comp
        
        mem_use_mb = mem_use/1024/1024
    
        return (mem_use_mb, mem_use)