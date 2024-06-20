#sort_tools module
#version 2
#now organized with a class structure
#file contains Four classes: Sort_tools, Db_tools, and Data_construct and Archive
#Sort_tools contains functions for munging and manipulating data - these are the best versions and most useful functions I've developed
#Db_tools contains functions for interacting with database and solving db related problems
#Data_construct is for analyses requiring multiple data files it scans a folder, loads csv and parquet files into dataframes, and organizes them in a dictionary structure
#Archive is for older functions and versions - I keep them around for reference, but don't want to import them routinely, as most have been supersceded by better ways of doing things.

import pandas as pd
import numpy as np
import xlwings as xw
import os
import cx_Oracle
from sqlalchemy.engine import create_engine
from db_pass import admin_pass, admin_name
import datetime as dt

class Sort_tools:

    @staticmethod
    def pt_date_interval_v4(df:object, id_str:str, date_str:str)->object:
        '''
        This is essentially an improved, more pythonic rewrite of the first version of this function.
        v4 takes one less parameter, returns a df, and uses pandas groupby and vector operations instead of iteration. 
        The df required should contain a list of patient ids and scan dates, such that there is one line per scan date 
        (duplicates will be dropped). Dates should be typed as datetime.
        Return fields:
        unique_date_list - all the scan dates per patient (list)
        first_date - earliest scan date per patient
        last_date - last scan date per patient
        date_count - count of scan dates
        LongestIntDays - days between first and last scans
        '''
        groups=df[[id_str, date_str]].copy()

        groups=groups.drop_duplicates()
        groups=groups.sort_values(by=[id_str, date_str])
        groups['converted_dates']=pd.to_datetime(groups[date_str].astype(str), format='%Y-%m-%d')
        groups['converted_dates']=groups['converted_dates'].dt.strftime('%Y%m%d')

        groups=groups.groupby(id_str).agg(
            unique_date_list=pd.NamedAgg(column='converted_dates', aggfunc=list),
            first_date=pd.NamedAgg(column=date_str, aggfunc='min'),
            last_date=pd.NamedAgg(column=date_str, aggfunc='max'),
            date_count=pd.NamedAgg(column=date_str, aggfunc='count')
        )
        groups=groups.reset_index()
        groups['LongestIntDays']=(groups['last_date']-groups['first_date'])/pd.Timedelta(days=1)
        groups['LongestIntDays']=groups['LongestIntDays'].astype(int)
    
        groups=groups[[id_str, 'first_date', 'last_date', 'date_count', 'LongestIntDays', 'unique_date_list']].copy()    
        return groups
    

    @staticmethod
    def print_unique(df:object, key_list:list) -> None:
        
        '''Takes a list of keys from a df, iterates through and prints unique values for each'''
        
        for key in key_list:
        
            u_vals = set(df[key].to_list())
        
            print(f'unique values in column {key} are: {u_vals}')

        return

    @staticmethod        
    def find_unique(dataframe:object, key:str) -> set:

        '''This function takes a dataframe and key, 
        and returns a set of unique values from that column of the df'''
        
        #send the dataframe[key] column to a list
        fun_list = dataframe[key]
        
        #use set() to find unique values in the list
        fun_set = set(fun_list)
        
        return fun_set

    @staticmethod
    def unique_val_table(df:object, id_string:str) -> object:
        
        '''Takes a table with repeating values, drops dupes, resets index and sets up an integer key with 
        the column title given by the id_string. df should be a dataframe, and id_string should be a string '''
        
        #find unique values and scrub initial index values

        df = df.drop_duplicates()

        df = df.reset_index()

        df = df.drop(columns = ['index'])
        
        #adding 1 so that I can index from one instead of zero
        extnt = len(df)+1
        
        #set up a key value using a series from numpy
        df[id_string] = np.arange(1, extnt)
        
        return df

    @staticmethod
    #switcheroo_v3 is the best and fastest function I've written for swapping key values
    def switcheroo_v3(translate_df:object, target_df:object, target_key:str) -> object:
        
        '''Use a traslator table between two keys to swap keys

        Translate_df: df with one column for the desired key, and one for the old key.
        
        This function is way better for long columns than v2
        '''
        
        target_df = pd.merge(translate_df, target_df, on = target_key, how = 'right')
        
        target_df = target_df.drop(columns = [target_key])
        
        return target_df 

    @staticmethod
    #use merges to count matching values
    def count_matching(df1:object, df2:object, match_key:str) -> int:
        
        '''Returns a count based on the records that match in two different dataframes '''
        
        match = pd.merge(df1, df2, how = 'inner', on = match_key)
        
        match_count = len(match)
        
        return match_count


    @staticmethod
    #use this function to iteratively filter dfs
    def filter_base(df:object, lower_bound:int, upper_bound:int, key_string:str) -> object:
        
        '''filters a dataframe based on a key string and interger lower and upper bounds'''
        
        age_bound = df[(df[key_string] >= lower_bound) & (df[key_string] < upper_bound)]
        
        return age_bound

    @staticmethod
    def count_groups(df:object, group_key:str, count_key:str) -> object:
        
        '''
        Does a groupby calculation to enumerate the members of a group in a particular dataframe
        
        Returns a dataframe in the format I generally find desirable for this kind of analysis
        
        '''
        
        prune_df = df[[group_key, count_key]].copy()        
        prune_df = prune_df.groupby(group_key).count()        
        prune_df = prune_df.reset_index()        
        prune_df = prune_df.rename(columns = {count_key: 'count'})
        
        return prune_df
   
    @staticmethod
    def substring_filter_v3(df:object, ta_key:str, keywords:list, delimiter:str, exclude:bool) -> object:
            
        '''
        Filter a dataframe using substrings in a string column.
            
        df: the dataframe to process
            
        ta_key: key corresponding to your column of string values
            
        keywords: a list of strings. Rows whose index include any of these substrings in the ta_key column will be flagged
        for inclusion or exclusion in the dataframe returned by the function
            
        delimiter: value that separates substrings in the string column

        exclude: If True, the filtration will remove rows lacking the keywords

        better version of substring_filter - this one uses the 'in' keyword instead of matching words in a list 
        split by the delimiter, so word combinations containing the delimiter can be passed in the keywords list
            
        '''
        #add the array to a list
        df_list = df[ta_key].to_list()        
        #lower the case of the keyword
        keywords = [key.lower() for key in keywords]        
        #setup a list for a binary value referring to the presence or absence of the keyword
        key_binary = []        
        #prepare the key binary list to hold a pass or fail score for presence of the keyword    
        for l in df_list:            
            #decompose on the delimiter
            string_comps = l.split(delimiter)            
            #lower the case
            string_comps = [s.lower() for s in string_comps]            
            #look for the keywords in string comps
            #rejoin so that multiple word keywords are possible
            string=delimiter.join(string_comps)
            key_bit = 0            
            for k in keywords:            
                if k in string:                
                    key_bit = 1                    
            key_binary.append(key_bit)                
        #add key_binary to the dataframe as a new column
        df['key_binary'] = key_binary        
        #filter for key_binary based on mode
        if exclude == True:
            df = df[df['key_binary'] == 0]
        else:
            df = df[df['key_binary'] == 1]        
        #drop the key_binary column
        df = df.drop(columns = ['key_binary'])        
        return df

    @staticmethod
    def string_appender(column:list, subst:str) -> list:
    
        '''
        This function is for manipulating a list of mixed keys or numeric keys to convert to string keys.
        numeric values in the input list ('column') will have the substring value ('subst') appended. 
        The function will transfer null values, without modifying them.
        The column has to be set as dtype = str before this function will work. 
        '''
        
        out_list = []
        
        for name in column:

            
            if name is None:
                out_list.append(None)
            
            elif name.isnumeric():            
                nname = f'{subst}{name}'            
                out_list.append(nname)
                
            else:
                out_list.append(name)
            
        return out_list

    @staticmethod
    def fix_mrn(mrn_list:list) -> list:
    
        '''
        Takes MRN list in and fixes the format so that there are a correct number of leading zeros

        The MRN list needs to be typed as int beforehand, or string, without a decimal        
        '''
        
        fixed_mrn = []
        
        #iterate through the mrn list
        for mrn in mrn_list:
            
            #calculate zeroes needed
            mrn_string = str(mrn)
            
            zeros_needed = 9 - len(mrn_string)
            
            #add zeros to the front of the mrn until there are 9 digits
            for n in range(zeros_needed):            
                mrn_string = str(0) + mrn_string
                
            #add fixed mrn to the list            
            fixed_mrn.append(mrn_string)
            
        return fixed_mrn

    @staticmethod
    def dframe_a_sheet(sheet_name:str, cell_range:str, file_path:str) -> object:

        '''
        Creates a pandas dataframe from an excel sheet
        '''
    
        #create an xw Book object from the file path
        db_book = xw.Book(file_path)

        #create a sheet object for a sheet in bht book db_book
        dummy_sheet = db_book.sheets[sheet_name]
        
        #dataframe it
        dummy_df = dummy_sheet[cell_range].options(pd.DataFrame, index = False, header = True).value
        
        #return the dataframe
        return dummy_df
    
    @staticmethod
    def dframe_a_sheet_v2(sheet_name:str, file_path:str) -> object:
        '''
        This better version of dframe_a_sheet doesn't need the shape of the data in your sheet
        It assumes your data starts on A1
        '''    
        #create an xw Book object from the file path
        db_book = xw.Book(file_path)

        #create a sheet object for a sheet in bht book db_book
        dummy_sheet = db_book.sheets[sheet_name]
        
        #dataframe it
        dummy_df = dummy_sheet.range('A1').expand().options(pd.DataFrame, index = False, header = True).value
        
        #return the dataframe
        return dummy_df

    @staticmethod
    def folder_list(f_path:str) -> list:
    
        '''This function grabs a list of folders at a given path'''
    
        sublist = []

        for entry in os.scandir(f_path):
        
            if entry.is_dir():
            
                sublist.append(entry.name)
    
        return sublist
    
    @staticmethod
    def calculate_current_age_column(date_list:list)->list:
        '''calculates a current age at runtime given a birth date column'''
        ct = dt.datetime.now()
        ages  = [ct - date for date in date_list]
        ages_years = [round(pd.to_numeric(age.days)/365, 2) for age in ages]
        return ages_years
    
    @staticmethod
    def remove_null_lines(df:object, non_numeric_keys:list)->object:
        '''
        Filters out lines in a DataFrame in which all numeric fields are null, but leaves lines with only some null values
        df - the dataframe to filter
        non_numeric_keys: a list of the keys to exclude
        '''    
        id_df = df[non_numeric_keys].copy()
        numeric_df = df.drop(columns=non_numeric_keys)
        row_sum = numeric_df.sum(axis=1)
        numeric_df['row_sum']= row_sum.to_list()
        numeric_df = numeric_df[numeric_df['row_sum']>0]
        numeric_df = numeric_df.drop(columns=['row_sum'])
        df = pd.merge(id_df, numeric_df, left_index=True, right_index=True, how='inner')
        df = df.reset_index()
        df = df.drop(columns=['index'])
        print(f'{len(id_df)-len(df)} null lines removed')
        return df
    
    @staticmethod
    def pd_display_ops(column_pref:int, row_pref:int)->None:
        '''
        Sets up the column and row visibility in dataframes for this sheet
        column_pref: columns visible. Use any int or None for infinity
        row_pref: rows visible. Use any int or None for infinity 
        '''
        pd.set_option('display.max_columns', column_pref)
        pd.set_option('display.max_row', row_pref)

    @staticmethod
    def count_nulls(df:object, count_keys:list, ids:list )->object:
        '''
        This function returns a dataframe with relevant ID keys and a count of nulls in fields not identified as ids
        df: dataframe housing data for analysis
        count_keys: a list of keys from which the null count will be compiled
        ids: a list of keys, which should be excluded from null analysis, and passed into the output dataframe
        '''
        count_df=df[count_keys].copy()
        #go through the keys - swap null for 1 and values for 0
        for key in count_df.keys():
            #convert values that aren't null to 0
            value_zapper=count_df[key].isnull()
            count_df[key]=count_df[key].where(value_zapper, 0)
            count_df[key]=count_df[key].fillna(1)
        
        #comute a row sum for the rows of the dataframe
        row_sum = count_df.sum(axis=1)
        output_frame=df[ids].copy()
        output_frame['relevant_nulls']=row_sum.to_list()
        return output_frame
    
    @staticmethod
    def lower_text_columns(df:object, column_list:list)->object:
        '''
        Lower the case of a string column in a dataframe
        Returns an identical dataframe to the one accepted, with listed string columns lowered in case
        df: a dataframe 
        column_list: list of columns for case lowering
        '''
        for column in column_list:
            text_list=df[column].to_list()
            new_list=[]
            for text in text_list:
                new_text=text.lower()
                new_list.append(new_text)
            df[column]=new_list
        return df
    
    @staticmethod
    def construct_longitudinal_df(interval_df:object, 
                              merge_df:object,
                              merge_date_key,
                              id_key:str,
                              diameter_key:str,
                              ct_count:int, 
                              strict_count_match:bool,
                              always_use_last_date:bool)->object:
        '''
        Constructs a dataframe with multiple dates and diameters per patient id
    
        interval_df: an output from the pt_date_interval function. Supplies a list of ct dates, a date count, and pt_id column
    
        merge_df: supplies diameters with associated measure dates

        merge_date_key: date key in merge df
    
        id_key: a patient id column common to both dfs
    
        diameter_key: key for diameter column (can be used for other measurements)
    
        ct_count: how many CTs are going to be tabulated
    
        strict_count_match: if True only people with a number of cts equal to ct_count will be included. if False, all people with 
        at least ct_count scans will be included - i.e. if a person has 5 scans they will be included if ct_count==2 and 
        strict_count_match==False but not if it is True
    
        always_use_last_date: if True the last ct date and diameter will be the ones at the end of the list instead of the next
        chronologically. This is only relevant if strict_count_match==False. Can be used for longest intervals if ct_count==2
        and strict_count_match==False
    
        Returns: a dataframe with numbered date and diameter columns by patient id
        '''
        if strict_count_match==True:
            rel_df=interval_df[interval_df.date_count==ct_count]
        else:
            rel_df=interval_df[interval_df.date_count>=ct_count]
        
        date_lists=rel_df['unique_date_list'].to_list()
    
        #make a dictionary for column headings and values
        date_cols={}
    
        for n in range(ct_count):
            col_num=n+1
            col_key=f'ct_date_{col_num}'
            col_vals=[]
        
            #iterate through the date list and pull out the nth value
            for date_list in date_lists:
                if always_use_last_date==True and col_num==ct_count:
                    col_vals.append(date_list[-1])
                else:
                    col_vals.append(date_list[n])            
            
            date_cols.update({col_key:col_vals})
        
        #pare the rel_df down in preparation for adding columns
        rel_df=rel_df[[id_key]].copy()
        
        #add columns and type
        for key in date_cols.keys():
            rel_df[key]=date_cols[key]
            rel_df[key]=pd.to_datetime(rel_df[key])
        
       
        mdf=merge_df[[id_key, merge_date_key, diameter_key]].copy()
    
        #insure that merge_date_key is correctly typed
        mdf[merge_date_key]=pd.to_datetime(mdf[merge_date_key])
        
        #merge together a dataset
        for key in date_cols.keys():
            rel_df=rel_df.rename(columns={key:merge_date_key})
            rel_df=pd.merge(rel_df, mdf, on=[id_key, merge_date_key], how='inner')
            key_number=key.split('_')[-1]
            new_diam_key=f'{diameter_key}_{key_number}'
            rel_df=rel_df.rename(columns={diameter_key:new_diam_key, merge_date_key:key})
        
        rel_df=rel_df.drop_duplicates()
    
        print(f'Product dataframe length: {len(rel_df)}')    
        
        return rel_df
    
    @staticmethod
    def remove_multiple_instance(df:object, id_key:str, date_key:str, num_key:str)->object:
        '''
        Removes multiple instances from a dataframe. It's specific to cases where one instance is expected.
        id_key and date_key are two keys for a groupby calculation, they can be any two keys. 
        num key is a third key that will be used for dertermining multiplicity.
        This can be used with longitudinal dfs after they are created.
        '''
        group_encs=df[[id_key, date_key, num_key]].copy()
        group_encs=group_encs.groupby([id_key, date_key]).count()
        group_encs=group_encs.reset_index()
        init_len=len(group_encs)
        print(f'Found {init_len} unique keys')
        group_encs=group_encs[group_encs[num_key]==1]
        fin_len=len(group_encs)
        d_len=init_len-fin_len
        print(f'removed {d_len} for multiple instance')
        id_cut=group_encs[[id_key]].copy()
        cut_df=pd.merge(df, id_cut, on=id_key, how='inner')
        print(f'final df contains {len(cut_df)} lines')
        return cut_df
    
    @staticmethod
    def calculate_deltas_and_rates(df:object, date_string_stem:str, diameter_string_stem:str)->object:
        '''
        Accepts a df produced by construct_longitudinal_df with multuple date and diameter keys, calculates
        deltas, intervals and rates from the diameters and dates. All date and diameter columns need to follow a form
        stem_stem_number (where you can have as many stems as you want separated by underscores, and number comes last)
        date and diameter strings need to be sequentially numbered.
    
        date_string_stem: a substring that will identify date keys eg. 'date' for ct_date_1 and ct_date_2
        diameter_string_stem: same but with diameters eg 'mid' for mid_asc_1 and mid_asc_2
    
        returns the input dataframes with sequentially numbered delta, interval (years), and rate fields
        '''
    
        #get date and diameter key lists
        date_keys=[key for key in df.keys() if date_string_stem in key]
        diameter_keys=[key for key in df.keys() if diameter_string_stem in key]
    
        #calculate deltas
        for n, diameter in enumerate(diameter_keys):
            d_key_comps=diameter.split('_')
            diameter_num=d_key_comps[-1]
            if int(diameter_num)==len(diameter_keys):
                break
            delta_key=f'delta_{diameter_num}'
            d_key_comps.pop(-1)
            diam_stem='_'.join(d_key_comps)
            diam1=f'{diam_stem}_{int(diameter_num)+1}'
            df[delta_key]=df[diam1]-df[diameter]
        
        #calculate intervals
        for n, date in enumerate(date_keys):
            da_key_comps=date.split('_')
            date_num=da_key_comps[-1]
            if int(date_num)==len(date_keys):
                break
            interval_key=f'interval_{date_num}'
            da_key_comps.pop(-1)
            date_stem='_'.join(da_key_comps)
            date1=f'{date_stem}_{int(date_num)+1}'
            df[interval_key]=(df[date1]-df[date])/pd.Timedelta(days=365)
        
        #get delta and interval keys
        deltas=[key for key in df.keys() if 'delta' in key]
        intervals=[key for key in df.keys() if 'interval' in key]
        deltas.sort()
        intervals.sort()
        assert len(deltas)==len(intervals), 'column numbers should match'
    
        #calculate rates
        for d, i in zip(deltas, intervals):
            rate_num=d.split('_')[-1]
            rate_key=f'rate_{rate_num}'
            df[rate_key]=df[d]/df[i]
        
        return df
    
    @staticmethod
    def prune_tight_dates(df:object, id_key:str,  date_key:str, space_factor:int)->object:
        '''
        This function is for sampling closely spaced time series data. It accepts a dataframe with an id key and date key along with a spacing factor in 
        days. The function aggregates dates by the id key, and removes dates subsequent to the first that are within the spacing factor, resetting when the
        time interval has exceeded the spacing factor. The return is a dataframe with id keys and date keys that are disaggregated and observe the spacing 
        factor within each id. 
        df - the dataframe to be spaced
        id_key - an ID to which dates are subordinate
        date_key - the date key containing dates to be spaced. Dates should be pretyped to datetime64
        '''
        
        #get only useful fields
        use_df=df[[id_key, date_key]].copy()
        #sort
        use_df=use_df.sort_values(by=[id_key, date_key])
        #group
        use_df=use_df.groupby(id_key).agg(
            date_list= pd.NamedAgg(column=date_key, aggfunc=list)
        )
        use_df=use_df.reset_index()
        viable_ids=[]
        viable_dates=[]
        id_list=use_df[id_key].to_list()
        #list2 because these are lists of lists
        date_list2=use_df['date_list'].to_list()
        #parallel iterate the lists
        for i, dlist in zip(id_list,  date_list2):
            #counter to prevent overpruning by chain pruning
            sumdelta=0
            #paralell iterate elist and dlist for pruning
            for n, d in enumerate(dlist):
                #test for first membership
                if n==0:
                    viable_ids.append(i)
                    viable_dates.append(d)
                else:
                    #calculate difference
                    delta=d-dlist[n-1]
                    #convert delta from timedelta to a day valued int
                    delta=pd.to_numeric(delta.days, downcast = 'integer')
                    #add to sumdelta in case the last value was rejected, but this one is further 
                    #from last kept value than spacer value
                    sumdelta+=delta
                    if (delta>space_factor) or (sumdelta>space_factor):
                        viable_ids.append(i)
                        viable_dates.append(d)
                        #value kept, reset counter
                        sumdelta=0                    
                        
        #return a new dataframe
        return_df=pd.DataFrame({id_key:viable_ids, date_key:viable_dates})
        torched_enc=len(df)-len(return_df)
        print(f'{torched_enc} removed for close proximity to most recent value')
        print(f'{len(return_df)} remain in the product dataframe')
        return return_df
    
@staticmethod
def calculate_date_intervals(df:object, id_key:str, date_key:str, longest_int:bool)->object:
    '''
    Calculate time intervals for a cohort. This function aggregates dates on a single patient Id and then calculates combinations in the form 
    id_d1_d2, id_d2_d3... id_dn_dn+1. A longest inerval is available as an option. a disaggregated dataframe is returned with id, time1, time2, 
    an interval name of the fom id_d1_d2, and interval length in years. This function was used to simulate VDM intervals
    df: dataframe containing relevant information
    id_key: the string name of the id_column
    date_key: date column
    longest int: a boolean value, True if longest intervals should be calculated. 
    '''
    grp_df=df[[id_key, date_key]].copy()
    grp_df=grp_df.groupby(id_key).agg(
        dates=pd.NamedAgg(column=date_key, aggfunc=list)
    )
    grp_df=grp_df.reset_index()
    pts_unique=grp_df[id_key].to_list()
    lol=grp_df['dates'].to_list()

    pt_id=[]
    intervals=[]
    interval_length=[]
    date1=[]
    date2=[]
    for pt, d_list in zip(pts_unique, lol):
        d_list.sort()
        date_iters=len(d_list)-1
        counter=0
        while date_iters>0:
            d1=d_list[counter]
            d2=d_list[counter+1]
            ds1=f"{d1.strftime('%Y')}{d1.strftime('%m')}{d1.strftime('%d')}"
            ds2=f"{d2.strftime('%Y')}{d2.strftime('%m')}{d2.strftime('%d')}"
            predict_string = f'{pt}_{ds1}_{ds2}'
            interval=round((d2-d1)/pd.Timedelta(days=365), 2)
            intervals.append(predict_string)
            interval_length.append(interval)
            pt_id.append(pt)
            date1.append(d1)
            date2.append(d2)
            counter+=1
            date_iters-=1
        if (len(d_list)>2) and longest_int==True:
            d1=d_list[0]
            d2=d_list[-1]
            ds1=f"{d1.strftime('%Y')}{d1.strftime('%m')}{d1.strftime('%d')}"
            ds2=f"{d2.strftime('%Y')}{d2.strftime('%m')}{d2.strftime('%d')}"
            predict_string = f'{pt}_{ds1}_{ds2}'
            interval=round((d2-d1)/pd.Timedelta(days=365), 2)
            intervals.append(predict_string)
            interval_length.append(interval)
            pt_id.append(pt)
            date1.append(d1)
            date2.append(d2)
    #construct df
    return_df=pd.DataFrame({id_key:pt_id, 'date1':date1, 'date2':date2, 'interval':intervals, 'interval_length_yrs':interval_length})
    return return_df 


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
    
    

class Data_construct:
    '''
    Data constructs are for loading and organizing dataframes into a notebook
    The Data construct requires a path to a folder that contains multiple data files
    The data files will be indexed in a dictionary by whatever shows up after the last underscore in their name
    Data files will be loaded into dataframes indexed in a dictionary with the same keys as the paths
    csv and parquet files are supported

    '''
    def __init__(self, root_path):
        self.root=root_path
        self.data_list=os.listdir(self.root)
        self.data_paths={}
        self.dfs={}
        
    def process_paths(self):
        print("*** Finding Data Paths ***")
        for datum in self.data_list:
            #get rid of folders
            if len(datum.split('.'))>1:
                dname = datum.split('.')[0]
                dict_key=dname.split(' ')[-1].split('_')[-1]
                d_path=os.path.join(self.root, datum)
                print(f'Adding {dict_key}:{d_path} to analysis')
                self.data_paths.update({dict_key:d_path})
                
    def load_dataframes(self):
        print("*** Loading Data Paths ***")
        for key in self.data_paths.keys():
            test_string = self.data_paths[key].split('.')[-1]
            if test_string =='gzip':
                df = pd.read_parquet(self.data_paths[key])
                print(f'{key} loaded from parquet')
                self.dfs.update({key:df})
            elif test_string=='csv':
                df=pd.read_csv(self.data_paths[key])
                print(f'{key} loaded from csv')
                self.dfs.update({key:df})


class Archive:
    @staticmethod
    def pt_date_interval(pt_list:list, evnt_tbl_pt_list:list, date_list:list) -> tuple:
    
        '''
        calculate procedure intervals and retrieve a list of procedure dates for a list of patients
    
        args:
        pt_list - a filtered patient list with more than 2 ct scans or some other criterion possibly    
        evnt_tbl_pt_list - a full list of patient ids from a list of procedures or other events    
        date_list - the corresponding list of dates (datetime values) from those events
    
        returns:
        delta_list - a list that holds the number of days between the first and last procedure or event for a patient, as an integer    
        pt_date_list - the dates of each CT scan or other procedure for each patient. Returned as a list of strings for each patient
    
        '''
        #start by generating a list for interval deltas and dates
        pt_date_list = []        
        delta_list = []
        
        #start by looking through the target patient list
        for pt in pt_list:            
            #make a list for relevant dates
            pt_dates = []             
            #initialize counter            
            counter = 0
            
            #iterate through the pt_id_coulumn
            for p in evnt_tbl_pt_list:                
                #look for a match with the current patient number
                if pt == p:                    
                    #store the information from the date column in a variable when found
                    sig_dt = date_list[counter]                    
                    #append sig_dt to pt_dates
                    pt_dates.append(sig_dt)                    
                #count up each time loop runs to keep track of position
                counter += 1                
            #test for an empty list here
            if pt_dates:            
                #Order the dates chronologically (early to late)
                pt_dates.sort()            
                #Calcuate time elapsed from first to last
                time_el = pt_dates[-1] - pt_dates[0]                
                #convert time_el from timedelta to integer (days)
                time_el = pd.to_numeric(time_el.days, downcast = 'integer')                
                #format the dates as dates by converting to string
                pt_date_strings = []
                
                for d in pt_dates:
                    day = d.strftime('%d')
                    month = d.strftime('%m')
                    year = d.strftime('%Y')
                    date_st = f'{month}/{day}/{year}'
                    
                    pt_date_strings.append(date_st)        
                #add found dates to pt_date_list
                pt_date_list.append(pt_date_strings)            
                #append time elapsed to the return list
                delta_list.append(time_el)
                
            #in case the list is empty it will evaluate as false
            #if this happens I need placeholders to keep things lined up properly 
            
            else:
                pt_date_list.append('No Procedures Found')
                delta_list.append('N/A')                    
                    
        return (delta_list, pt_date_list)
    
    @staticmethod
    def pt_date_interval_v2(pt_list:list, evnt_tbl_pt_list:list, date_list:list, acc_list:list) -> tuple:
    
        '''
        This method is supersceded by v3
        calculate procedure intervals and retrieve a list of procedure dates for a list of patients
        
        args:
        pt_list - a filtered patient list with more than 2 ct scans or some other criterion possibly        
        evnt_tbl_pt_list - a full list of patient ids from a list of procedures or other events        
        date_list - the corresponding list of dates (datetime values) from those events        
        acc_list - a list of acc numbers for procedures
        
        returns:
        delta_list - a list that holds the number of days between the first and last procedure or event for a patient, 
        as an integer        
        pt_date_list - the dates of each CT scan or other procedure for each patient. Returned as a list of strings 
        for each patient        
        acc_dict_list - for each scan date this contains a dictionary of the form {date:[acc#, acc#]}        
        count_list - a count of CT scans for the patient
     
        
        '''
        #start by generating a list for interval deltas and dates
        pt_date_list = []            
        delta_list = []        
        acc_entry = []        
        count_list =[]
            
        #start by looking through the target patient list
        for pt in pt_list:                
            #make a list for relevant dates
            pt_dates = []            
            #make a list for relevant acc
            pt_acc = []                
            #initialize counter                
            counter = 0                
            #iterate through the pt_id_coulumn
            for p in evnt_tbl_pt_list:                    
                #look for a match with the current patient number
                if pt == p:                        
                    #store the information from the date column in a variable when found
                    sig_dt = date_list[counter]                    
                    #also get the acc#
                    sig_acc = acc_list[counter]                        
                    #append sig_dt to pt_dates
                    pt_dates.append(sig_dt)                    
                    #append acc# to pt_acc
                    pt_acc.append(sig_acc)                        
                #count up each time loop runs to keep track of position
                counter += 1                    
            #test for an empty list here
            if pt_dates:                
                #make a non-redunant date list
                pt_dt_unique = list(set(pt_dates))
                #sort so that the dates arent random
                pt_dt_unique.sort()                
                #construct the ACC# dict
                #iterate through the unique list
                acc_dict = {}
                for dt in pt_dt_unique:
                    #construct date key
                    day = dt.strftime('%d')
                    month = dt.strftime('%m')
                    year = dt.strftime('%Y')
                    key_string = f'{year}{month}{day}'                    
                    #look for matches to dt in pt_dates, and use it to get acc#
                    accs = []
                    for d, a in zip(pt_dates, pt_acc):
                        if d==dt:
                            accs.append(a)                            
                    acc_dict.update({key_string:accs})         
            
                #Calcuate time elapsed from first to last
                time_el = pt_dt_unique[-1] - pt_dt_unique[0]                    
                #convert time_el from timedelta to integer (days)
                time_el = pd.to_numeric(time_el.days, downcast = 'integer')                    
                #format the dates as dates by converting to string
                pt_date_strings = []                    
                for d in pt_dt_unique:
                    day = d.strftime('%d')
                    month = d.strftime('%m')
                    year = d.strftime('%Y')
                    date_st = f'{year}{month}{day}'                        
                    pt_date_strings.append(date_st)           
        
                #add found dates to pt_date_list
                pt_date_list.append(pt_date_strings)                
                #append time elapsed to the return list
                delta_list.append(time_el)                
                #append acc_dict to the dict list
                acc_entry.append(acc_dict)                
                #count the date list for count list
                count_list.append(len(pt_date_strings))
                    
            #in case the list is empty it will evaluate as false
            #if this happens I need placeholders to keep things lined up properly 
                
            else:
                pt_date_list.append('No Procedures Found')
                delta_list.append('N/A')
                acc_entry.append('N/A')
                count_list.append(0)                        
                        
        return (delta_list, pt_date_list, acc_entry, count_list)
    
    @staticmethod
    def pt_date_interval_v3(df:object, pt_col:str, date_col:str, acc_col:str)->object:
        '''
        This does the same thing as v2, but it accepts a pandas df with strings to designate column headings
        The function returns a pandas dataframe with stats on CT and columns for aggregated dates and acc#
        Use this version instead of v2 because it lacks vulnerabilities based on list ordering    
        '''
        #construct grouped acc frame
        grouped=df[[pt_col, date_col, acc_col]].copy()
        grouped=grouped.groupby([pt_col, date_col]).agg(list)
        grouped=grouped.reset_index()
        #unique patients list
        pts_unique=grouped[pt_col].to_list()
        pts_unique=list(set(pts_unique))
        #other lists
        pts=grouped[pt_col].to_list()
        dates=grouped[date_col].to_list()
        acc_lists=grouped[acc_col].to_list()
        #product lists
        ct_date_col=[]
        ct_acc_num_col=[]
        max_interval_col=[]
        count_col=[]
        for patient in pts_unique:
            date_entry=[]
            acc_entry={}
            for p, d, a in zip(pts, dates, acc_lists):
                if p==patient:
                    day = d.strftime('%d')
                    month = d.strftime('%m')
                    year = d.strftime('%Y')
                    date_string = f'{year}{month}{day}'
                    date_entry.append(d)
                    acc_entry.update({date_string:a})                
            if date_entry:
                #sort lists
                date_entry.sort()
                acc_entry=dict(sorted(acc_entry.items()))
                #calculate max interval
                time_el=date_entry[-1]-date_entry[0]
                time_el=pd.to_numeric(time_el.days, downcast = 'integer')
                #put string dates in a list
                date_list=[key for key in acc_entry.keys()]
                #append
                ct_date_col.append(date_list)
                ct_acc_num_col.append(acc_entry)
                max_interval_col.append(time_el)
                count_col.append(len(date_list))
            
            else:
                ct_date_col.append('N/A')
                ct_acc_num_col.append('N/A')
                max_interval_col.append('N/A')
                count_col.append(0)
            
        #write new dataframe
        new_df=pd.DataFrame({pt_col:pts_unique,
                            'ct_count':count_col, 
                            'MaxInterval':max_interval_col, 
                            'DateList':ct_date_col,
                            'AccByDate':ct_acc_num_col
                            })
        return new_df
    
    @staticmethod
    def aggregate_history(pt_list:list, e_t_pt_list:list, date_list:list, event_table:list)->list:
        '''
        This function aggregates a table of events with dates and patient ids. With this function, you can condence 
        a patient's history into a single line. 
        
        Parameters
        pt_list: a list of unique ids for a set of patients of interest
        e_t_pt_list: the id column from a table of historical events
        date_list: the date column from a table of historical events
        event_table: the event column from a table of historical events
        
        Returns
        dict list: a list of dictionaries with a dictionary for each patient in pt_list
        the dictionary is of the form {'date1':[event1, event2], 'date2':[event3] ... }

        This function is archived because it was built for a particular request that didn't become routine
        I could probably refactor this funciton using groupby, but regardless, I don't think the output was that helpful, and I probably
        won't ever do things like this very often. 
        '''    
        #this is for the final return list
        dict_list = []        
        #iterate through pt_list 
        for pt in pt_list:            
            #list for significant dates
            sig_date_lst = []            
            #list for procedure names
            proc_name_list = []            
            #loop through the e_t_pt_list
            for p, d, e in zip(e_t_pt_list, date_list, event_table):                
                #collect the data on this patient
                if pt == p:                
                    sig_date_lst.append(d)
                    proc_name_list.append(e) 

            if sig_date_lst:                
                #make a unique date list
                unique_dates = list(set(sig_date_lst))
                unique_dates.sort()   

                #construct the return dict
                event_dict = {}                
                for dt in unique_dates:
                    #construct date key
                    day = dt.strftime('%d')
                    month = dt.strftime('%m')
                    year = dt.strftime('%Y')
                    key_string = f'{year}{month}{day}'                    
                    #look for matches to dt in pt_dates, and use it to get acc#
                    events = []
                    for d, a in zip(sig_date_lst, proc_name_list):
                        if d==dt:
                            events.append(a)                            
                    event_dict.update({key_string:events})                    
                dict_list.append(event_dict)
                                
            else:
                dict_list.append('No Events Found')

        return dict_list
    @staticmethod
    def switcheroo_v2(key_list:list, value_list:list, input_list:list) -> list:
        
        ''' This function takes two ordered lists and a list to be transformed. The values in the input_list should match one
        of the ordered lists, and there should be no overlap between the contents of the two lists. The function switches a matching
        value with its counterpart in the other list. 
        
        This version only works well for short columns or arrays. It is very slow for large ones. 
        '''
        
        #instantiate a list for output
        out_list = []    
        
        #iterate through the list to look for values in the codex
        for i in input_list:     
            
            #iterate through the key lists and look for a match where i == k
            for k, v in zip(key_list, value_list):
                
                if k == i:
                    
                    #in case of a match append v
                    out_list.append(v)
                    
                elif v == i:
                    
                    #if no match is found see if v matches i. If it does append k
                    out_list.append(k)
                    
        return out_list
    
    @staticmethod
    def substring_filter(df:object, ta_key:str, keywords:list, delimiter:str) -> object:
        
        '''
        Filter a dataframe using substrings in a string column. Only include rows of a dataframe if a string column contains a certain substring. 
        
        df: the dataframe to process
        
        ta_key: key corresponding to your column of string values
        
        keywords: a list of strings. rows whose index include any of these substrings in the ta_key column will be included in
        the dataframe returned by the function
        
        delimiter: what value will substrings in the string column will be separated by

        originally called 'text_array_filter'
        
        '''
        
        #add the array to a list
        df_list = df[ta_key].to_list()
        
        #lower the case of the keyword
        keywords = [key.lower() for key in keywords]
        
        #setup a list for a binary value referring to the presence or absence of the keyword
        key_binary = []
        
        #prepare the key binary list to hold a pass or fail score for presence of the keyword    
        for l in df_list:
            
            #decompose on the delimiter
            string_comps = l.split(delimiter)
            
            #lower the case
            string_comps = [s.lower() for s in string_comps]
            
            #look for the keywords in string comps
            
            key_bit = 0
            
            for k in keywords:
            
                if k in string_comps:
                
                    #make key_bit = 1
                    key_bit = 1
                    
            key_binary.append(key_bit)

                
        #add key_binary to the dataframe as a new column
        df['key_binary'] = key_binary
        
        #filter for key_binary = 1
        df = df[df['key_binary'] == 1]
        
        #drop the key_binary column
        df = df.drop(columns = ['key_binary'])
        
        return df
    
    @staticmethod
    def substring_filter_v2(df:object, ta_key:str, keywords:list, delimiter:str, exclude:bool) -> object:
        
        '''
        Filter a dataframe using substrings in a string column.
        
        df: the dataframe to process
        
        ta_key: key corresponding to your column of string values
        
        keywords: a list of strings. Rows whose index include any of these substrings in the ta_key column will be flagged
        for inclusion or exclusion in the dataframe returned by the function
        
        delimiter: value that separates substrings in the string column

        exclude: If True, the filtration will remove rows lacking the keywords

        better version of substring_filter
        
        '''
        
        #add the array to a list
        df_list = df[ta_key].to_list()
        
        #lower the case of the keyword
        keywords = [key.lower() for key in keywords]
        
        #setup a list for a binary value referring to the presence or absence of the keyword
        key_binary = []
        
        #prepare the key binary list to hold a pass or fail score for presence of the keyword    
        for l in df_list:
            
            #decompose on the delimiter
            string_comps = l.split(delimiter)
            
            #lower the case
            string_comps = [s.lower() for s in string_comps]
            
            #look for the keywords in string comps
            
            key_bit = 0
            
            for k in keywords:
            
                if k in string_comps:
                
                    #make key_bit = 1
                    key_bit = 1
                    
            key_binary.append(key_bit)

                
        #add key_binary to the dataframe as a new column
        df['key_binary'] = key_binary
        
        #filter for key_binary based on mode
        if exclude == True:
            df = df[df['key_binary'] == 0]
        else:
            df = df[df['key_binary'] == 1]
        
        #drop the key_binary column
        df = df.drop(columns = ['key_binary'])
        
        return df 
    
    @staticmethod
    def count_loop(df_list:list, name_list:list, comp_df:object, match_key:str) -> object:
        
        '''
        create a dataframe with counts based on matching entries in another dataframe

        uses the count matching function

        There is nothing inherently wrong with this function, but it has a very limited use case
        
        '''
        def count_matching(df1:object, df2:object, match_key:str) -> int:
        
            '''Returns a count based on the records that match in two different dataframes '''
        
            match = pd.merge(df1, df2, how = 'inner', on = match_key)
        
            match_count = len(match)
        
            return match_count
        
        results_list = []
        
        for df in df_list:
            
            result = count_matching(df, comp_df, match_key)
            
            results_list.append(result)
            
        result_df = pd.DataFrame({'category': name_list, 'count': results_list})
        
        return result_df
   
