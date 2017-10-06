# -*- coding: utf-8 -*-
"""
Created on Sun Oct  1 17:30:47 2017

@author: lifen
"""
# =================================== Data Collection =============================
import requests
import pandas as pd
import numpy as np

# Endpoint of the first dataset
BaseURL = 'https://api.census.gov/data/timeseries/healthins/sahie'

# Parameters that we selected to collect (Part A, year = 2015)
URLPost = {'get': 'NAME,NIC_PT,NIC_LB90,NIC_UB90,NUI_PT,NUI_LB90,NUI_UB90',
           'for': 'county:*',
           'in': 'state:*',
           'time': '2015'
           }

# Scrape data with a json format
response = requests.get(BaseURL, URLPost)
jsontxt = response.json()

# Initialize a dataframe with column names assigned
df = pd.DataFrame(columns = ('County_State','Number_Insured','NInsured_CI_LowerBound','NInsured_CI_UpperBound','Number_Uninsured',
                             'NUninsured_CI_LowerBound','NUninsured_CI_UpperBound','time','State_Code','County_Code'))

# Append data to each column of the dataframe, skip the column name row
for i in range(1,len(jsontxt)):
    Location = jsontxt[i][0]
    NInsured = jsontxt[i][1]
    NIC_LB90 = jsontxt[i][2]
    NIC_UB90 = jsontxt[i][3]
    NUnisured = jsontxt[i][4]
    NUI_LB90 = jsontxt[i][5]
    NUI_UB90 = jsontxt[i][6]
    time = jsontxt[i][7]
    state = jsontxt[i][8]
    county = jsontxt[i][9]
    df = df.append({'County_State':Location,'Number_Insured':NInsured,'NInsured_CI_LowerBound':NIC_LB90,'NInsured_CI_UpperBound':NIC_UB90,
                    'Number_Uninsured':NUnisured,'NUninsured_CI_LowerBound':NUI_LB90,'NUninsured_CI_UpperBound':NUI_UB90,'time':time,
                    'State_Code':state,'County_Code':county},ignore_index=True)

# Parameters that we selected to collect (Part B, year = 2013)
URLPost = {'get': 'NAME,NIC_PT,NIC_LB90,NIC_UB90,NUI_PT,NUI_LB90,NUI_UB90',
           'for': 'county:*',
           'in': 'state:*',
           'time': '2013'
           }

# Scrape data with a json format
response = requests.get(BaseURL, URLPost)
jsontxt = response.json()

# Continue appending data to each column of the dataframe, skip the column name row
for i in range(1,len(jsontxt)):
    Location = jsontxt[i][0]
    NInsured = jsontxt[i][1]
    NIC_LB90 = jsontxt[i][2]
    NIC_UB90 = jsontxt[i][3]
    NUnisured = jsontxt[i][4]
    NUI_LB90 = jsontxt[i][5]
    NUI_UB90 = jsontxt[i][6]
    time = jsontxt[i][7]
    state = jsontxt[i][8]
    county = jsontxt[i][9]
    df = df.append({'County_State':Location,'Number_Insured':NInsured,'NInsured_CI_LowerBound':NIC_LB90,'NInsured_CI_UpperBound':NIC_UB90,
                    'Number_Uninsured':NUnisured,'NUninsured_CI_LowerBound':NUI_LB90,'NUninsured_CI_UpperBound':NUI_UB90,'time':time,
                    'State_Code':state,'County_Code':county},ignore_index=True)    

# Endpoint of the second dataset    
BaseURL1 = 'https://data.medicare.gov/resource/2kat-xip9.json'

# Parameters that we selected
URLPost1 = {'$limit':'20000'}

# Scrape data with a json format
response1 = requests.get(BaseURL1, URLPost1)
jsontxt1 = response1.json()

# Initialize a dataframe with column names assigned
df1 = pd.DataFrame(columns=('Provider_id','County','State','Lower_Payment_Est','Ave_Payment','Higher_Payment_Est',
                            'Start_Date','End_Date'))

# Append data to each column of the dataframe
for i in jsontxt1:
    try:
        County = i['county_name'] # Deal with data without 'county_name' dictionary key
    except:
        County = np.NaN # Deal with data without 'county_name' dictionary key
    State = i['location_state']
    Lower_Est = i['lower_estimate']
    Higher_Est = i['higher_estimate']
    Payment = i['payment']
    Start_Date = i['measure_start_date']
    End_Date = i['measure_end_date']
    ID = i['provider_id']
    Zip = i['zip_code']
    df1 = df1.append({'Provider_id':ID,'County':County,'State':State,'Lower_Payment_Est':Lower_Est,'Ave_Payment':Payment,
                      'Higher_Payment_Est':Higher_Est,'Start_Date':Start_Date,'End_Date':End_Date},ignore_index=True)

    
    
# =================================== Data Cleanliness =============================    
# Replace all format of missing value by np.NAN    
df = df.replace(('N/A','Not Available','',' '), np.NAN)
df1 = df1.replace(('N/A','Not Available','',' '), np.NAN)


def Cleanliness(df):
    # Fraction of missing values of each attributes
    Frac_NAN = df.isnull().sum(axis=0)/len(df)

    # Fraction of noise value of each attributes (exclude the missing values)
    df_N_1 = sum(df['County_State'].dropna().apply(lambda x: len(x.split(', ', 1))) != 2)/len(df) # detect any value cannot split into county and state
    df_N_2 = sum(df['Number_Insured'].dropna().apply(lambda x: not str(x).isdigit()))/len(df) # detect any value that doesn't only contains digits
    df_N_3 = sum(df['NInsured_CI_LowerBound'].dropna().apply(lambda x: not str(x).isdigit()))/len(df) # detect any value that doesn't only contains digits
    df_N_4 = sum(df['NInsured_CI_UpperBound'].dropna().apply(lambda x: not str(x).isdigit()))/len(df) # detect any value that doesn't only contains digits
    df_N_5 = sum(df['Number_Uninsured'].dropna().apply(lambda x: not str(x).isdigit()))/len(df) # detect any value that doesn't only contains digits
    df_N_6 = sum(df['NUninsured_CI_LowerBound'].dropna().apply(lambda x: not str(x).isdigit()))/len(df) # detect any value that doesn't only contains digits
    df_N_7 = sum(df['NUninsured_CI_UpperBound'].dropna().apply(lambda x: not str(x).isdigit()))/len(df) # detect any value that doesn't only contains digits
    df_N_8 = sum(df['time'].dropna().apply(lambda x: x not in ['2015','2013']))/len(df) # detect any value other than '2015' or '2013', which we set in the URLPost
    df_N_9 = sum(df['State_Code'].dropna().apply(lambda x: (len(x) != 2) or (not x.isdigit())))/len(df) # detect any value without a two-digit format
    df_N_10 = sum(df['County_Code'].dropna().apply(lambda x: (len(x) != 3) or (not x.isdigit())))/len(df) # detect any value without a three-digit format
    
    # Store the fractions into a serie
    Frac_Noise = pd.Series((df_N_1,df_N_2,df_N_3,df_N_4,df_N_5,df_N_6,df_N_7,df_N_8,df_N_9,df_N_10), index=('County_State','Number_Insured',
                           'NInsured_CI_LowerBound','NInsured_CI_UpperBound','Number_Uninsured','NUninsured_CI_LowerBound',
                           'NUninsured_CI_UpperBound','time','State_Code','County_Code'))
    
    # General cleanliness for each attributes by category in a dataframe format and print it
    Result = pd.DataFrame({'MissingValue':Frac_NAN,'NoiseValue':Frac_Noise})    
    print(Result)
    
    # Equal weight for all the attributes and cleanliness categories, and scale it to a 0-100 range
    Score = 100*(1-(Result.MissingValue.sum()+Result.NoiseValue.sum())/(2*len(Result)))
    print('\nCleanliness Score is',Score)

def Cleanliness1(df1):
    # Fraction of missing values of each attribute
    Frac_NAN1 = df1.isnull().sum(axis=0)/len(df1)
    
    # Fraction of noise value of each attributes (exclude the missing values)
    df1_N_1 = sum(df1['Provider_id'].dropna().apply(lambda x: (not x.isdigit()) or (len(x) != 6)))/len(df1) # detect any value without a 6-digit format
    df1_N_2 = sum(df1['County'].dropna().apply(lambda x: not any((y.isalpha() or y.isspace()) for y in x)))/len(df1) # detect any value that doesn't only contains digit and alpha
    df1_N_3 = sum(df1['State'].apply(lambda x: (not x.isalpha()) or len(x) != 2))/len(df1) # detect any value without a 2-alpha format
    df1_N_4 = sum(df1['Lower_Payment_Est'].dropna().apply(lambda x: any(y.isalpha() for y in str(x))))/len(df1) # detect any value contains alpha
    df1_N_5 = sum(df1['Ave_Payment'].dropna().apply(lambda x: any(y.isalpha() for y in str(x))))/len(df1) # detect any value contains alpha
    df1_N_6 = sum(df1['Higher_Payment_Est'].dropna().apply(lambda x: any(y.isalpha() for y in str(x))))/len(df1) # detect any value contains alpha
    df1_N_7 = sum(df1['Start_Date'].dropna().apply(lambda x: (x[4] != '-') or (x[7] != '-')))/len(df1) # detect any value not in a 'XXXX-XX-XX...' format
    df1_N_8 = sum(df1['End_Date'].dropna().apply(lambda x: (x[4] != '-') or (x[7] != '-')))/len(df1) # detect any value not in a 'XXXX-XX-XX...' format
    
    # Store the fractions into a serie
    Frac_Noise1 = pd.Series((df1_N_1,df1_N_2,df1_N_3,df1_N_4,df1_N_5,df1_N_6,df1_N_7,df1_N_8), index=('Provider_id','County','State',
                           'Lower_Payment_Est','Ave_Payment','Higher_Payment_Est','Start_Date','End_Date'))

    # General cleanliness for each attributes by category in a dataframe format and print it
    Result1 = pd.DataFrame({'MissingValue':Frac_NAN1,'NoiseValue':Frac_Noise1})
    print(Result1)
    
    # Equal weight for all the attributes and cleanliness categories, and scale it to a 0-100 range
    Score1 = 100*(1-(Result1.MissingValue.sum()+Result1.NoiseValue.sum())/(2*len(Result1)))
    print('\nCleanliness Score is',Score1)


Cleanliness(df)
Cleanliness1(df1)

# =================================== Data Cleanning =============================  
# Drop rows with missing values
DF = df.dropna()

# Make all values in lower cases
DF = DF.applymap(lambda x: x.lower())

# Drop rows with wrong county_state values
DF = DF[DF['County_State'].apply(lambda x: len(x.split(', ', 1)) == 2)]

# Delete ' county' in each value
DF['County_State'] = DF['County_State'].apply(lambda x: x.replace(' county', ''))

# Correct the type of number relative columns to be integer
DF[['Number_Insured','NInsured_CI_LowerBound','NInsured_CI_UpperBound','Number_Uninsured','NUninsured_CI_LowerBound',
    'NUninsured_CI_UpperBound']] = DF[['Number_Insured','NInsured_CI_LowerBound','NInsured_CI_UpperBound','Number_Uninsured',
                               'NUninsured_CI_LowerBound','NUninsured_CI_UpperBound']].astype('int64')

# Drop rows with missing values
DF1 = df1.dropna()

# Make all values in lower cases
DF1 = DF1.applymap(lambda x: x.lower())

# Delete '$', ',' and ' ' from payment related columns' values 
translator = lambda x: x.translate(str.maketrans(dict.fromkeys('$, ')))
DF1[['Lower_Payment_Est','Ave_Payment','Higher_Payment_Est']]=DF1[['Lower_Payment_Est','Ave_Payment','Higher_Payment_Est']].applymap(translator)

# Correct the type of number relative columns to be integer
DF1[['Lower_Payment_Est','Ave_Payment','Higher_Payment_Est']]=DF1[['Lower_Payment_Est','Ave_Payment','Higher_Payment_Est']].astype('int64')

# Correct the date format to be 'XXXX-XX-XX'
DF1[['Start_Date','End_Date']] = DF1[['Start_Date','End_Date']].applymap(lambda x: x[0:10])


Cleanliness(DF)
Cleanliness1(DF1)
