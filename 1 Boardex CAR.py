import pandas as pd
import re
import numpy as np
import math
from datetime import datetime, timedelta, date
import os
import time
import glob
from scipy.stats import mstats

pd.set_option('display.expand_frame_repr', False)
# pd.set_option('display.width', 400)
# pd.set_option('display.max_colwidth', -1)
# Reduce decimal points to 2
pd.options.display.float_format = '{:,.4f}'.format

# set my desired working directory
os.chdir() #pc

num_rows= None #=None for all sample

# Load raw boardex data, Source names in quatation marks:
boardex_raw_manager = pd.read_csv("BoardEx - Individual Profile Employment.csv", nrows=num_rows, encoding="ISO-8859-1",parse_dates=['DateStartRole'])
boardex_raw_company = pd.read_csv("BoardEx - Company Profile Details.csv", nrows=num_rows, encoding="ISO-8859-1")
boardex_raw_individual_profile = pd.read_csv("BoardEx - Individual Profile Details.csv",
                                             encoding="ISO-8859-1",
                                             usecols=['DirectorName', 'DirectorID', 'Gender', 'Nationality',
                                                      'NetworkSize'])
#filter country of first work of manager by sorting directorID and starting date, and then drop duplicates
manager_first_work = boardex_raw_manager.sort_values(["DirectorID","DateStartRole"]).drop_duplicates(subset=["DirectorID"])[["DirectorID","HOCountryName"]]
manager_first_work = manager_first_work.rename(columns={"HOCountryName":"FirstWorkCountry"}).dropna(subset=["FirstWorkCountry"]).reset_index(drop=True)

# Define the mapping of countries to their corresponding nationalities, defaulting to "Other country" or "United States" where not specified
country_to_nationality = {
    'United States': 'American','United Kingdom - England': 'British','United Kingdom - Scotland': 'British',
    'United Kingdom - Unknown': 'British','United Kingdom - Northern Ireland': 'British','United Kingdom - Wales': 'British', 'Netherlands': 'Dutch','Germany': 'German','Italy': 'Italian',
    'Belgium': 'Belgian','Spain': 'Spanish','Brazil': 'Brazilian','India': 'Indian','Sweden': 'Swedish','Canada': 'Canadian','Chile': 'Chilean',
    'China': 'Chinese','Taiwan Territory of': 'Chinese (Taiwan)','France': 'French','Colombia': 'Colombian','Croatia': 'Croatian',
    'Greece': 'Greek','Korea Republic Of (South Korea)': 'South Korean','Japan': 'Japanese','Republic Of Ireland': 'Irish','Mexico': 'Mexican','Pakistan': 'Pakistani',
    'Russian Federation': 'Russian','Singapore': 'Singaporean','Australia': 'Australian','Hong Kong SAR': 'Chinese','Macao SAR': 'Chinese',
    'Turkey': 'Turkish','Philippines': 'Filipino','Kenya': 'Kenyan','Norway': 'Norwegian','Ireland': 'Irish','Morocco': 'Moroccan',
    'United Arab Emirates': 'Emirian','Malaysia': 'Malaysian','Qatar': 'Qatari','Saudi Arabia': 'Saudi','Puerto Rico': 'American','Peru': 'Peruvian','Ethiopia': 'Ethiopian',
    'Israel': 'Israeli','Denmark': 'Danish','Ukraine': 'Ukrainian','Romania': 'Romanian','Nigeria': 'Nigerian','Thailand': 'Thai','Switzerland': 'Swiss',
    'Argentina': 'Argentine','Venezuela': 'Venezuelan','Portugal': 'Portuguese','Indonesia': 'Indonesian','Poland': 'Polish','Jordan': 'Jordanian','Afghanistan': 'Afghan',
    'Sudan': 'Sudanese','Bangladesh': 'Bangladeshi'
}
#convert to Citinzship Format
manager_first_work['FirstWorkCountry'] = manager_first_work['FirstWorkCountry'].map(lambda x: country_to_nationality.get(x, x))
#Drop Uknown values
manager_first_work = manager_first_work[manager_first_work['FirstWorkCountry'] != 'Unknown'].reset_index(drop=True)

#merge first work country to manager
boardex_raw_individual_profile = pd.merge(boardex_raw_individual_profile, manager_first_work, on='DirectorID', how='left')

del manager_first_work

#fill missing Nationality with FirstWorkCountry
boardex_raw_individual_profile['NationalityORG'] = boardex_raw_individual_profile['Nationality'].copy()
boardex_raw_individual_profile['Nationality'] = np.where(boardex_raw_individual_profile['Nationality'].isna(), boardex_raw_individual_profile['FirstWorkCountry'], boardex_raw_individual_profile['Nationality'])

boardex_raw_individual_profile.to_csv('boardex_nationality_processed.csv',index=False) #save nationality data


##Manager has sector experience##
# Ensure the dates are in datetime format
prior_experience = boardex_raw_manager[['DirectorID','DateStartRole','CompanyID','Sector']]
prior_experience = prior_experience.sort_values(['DirectorID','DateStartRole'])
prior_experience = prior_experience.drop_duplicates(subset=['DirectorID','CompanyID'],keep='first')
# Filter out records where sector information is missing or not matching the firm's sector
prior_experience = prior_experience.dropna(subset=['Sector',"DateStartRole"])
#give random num to sector
np.random.seed(0)  # For reproducibility
unique_sectors = prior_experience['Sector'].unique()
sector_to_int = {sector: np.random.randint(1, 60) for sector in unique_sectors}
prior_experience['SectorInt'] = prior_experience['Sector'].map(sector_to_int)
#generate a list of prior sectors
prior_experience['CumulativeSectors'] = prior_experience.groupby('DirectorID')['SectorInt'].apply(lambda x: [list(set(x.iloc[:i])) for i in range(len(x))]).explode().tolist()
#Determine if the director had prior experience in the same sector
prior_experience['PriorExperience'] = prior_experience.apply(lambda row: row['SectorInt'] in row['CumulativeSectors'], axis=1).astype(int)
#merge to main data
boardex_raw_manager = pd.merge(boardex_raw_manager,prior_experience[['DirectorID','CompanyID','DateStartRole','PriorExperience','CumulativeSectors']],on=['DirectorID','CompanyID','DateStartRole'],how='left')
boardex_raw_manager.sort_values(['DirectorID','DateStartRole'],inplace=True)
boardex_raw_manager['PriorExperience'] = boardex_raw_manager.groupby(['DirectorID', 'CompanyID'])['PriorExperience'].transform('ffill')
boardex_raw_manager['CumulativeSectors'] = boardex_raw_manager.groupby(['DirectorID', 'CompanyID'])['CumulativeSectors'].transform('ffill')

del prior_experience

#list of terms I would like to look
manager_string_SCM = ['SCO', 'SCM','sco','scm','Logistics','Purchasing','Supply','supply','Sco','Scm','procurement','Procurement','Sourcing','sourcing']  # list of terms I would like to look
manager_string_COO = ['COO', 'Operations','operations','coo','OPS','ops','Ops','Coo']

# merge CIK code to each manager-firm
boardex_raw_company = boardex_raw_company.dropna(subset=['CIKCode'])  # drop missing CIK
boardex_raw_company = boardex_raw_company[
    ['BoardName', 'BoardID', 'CIKCode', 'Ticker', 'CCCountryName']]  # leave only relevant variables
boardex_raw_manager_company = pd.merge(left=boardex_raw_manager, right=boardex_raw_company, left_on='CompanyID',
                                       right_on='BoardID', how='left')  # merge to have CIK code
boardex_raw_manager_company = boardex_raw_manager_company.dropna(subset=['CIKCode'])  # drop missing CIK
boardex_raw_manager_company = boardex_raw_manager_company.drop_duplicates(
    subset=['CompanyID', 'DirectorID', 'RoleName', 'DateStartRole', 'DateEndRole'])  # drop_duplicates
# merge nationality to each board member
boardex_raw = pd.merge(left=boardex_raw_manager_company, right=boardex_raw_individual_profile, on='DirectorID',
                       how='left')  # merge to have CIK code

##arrange data##
boardex_raw['DateEndRole'] = boardex_raw['DateEndRole'].replace('C','20200501')  ##replace current directors with end may date

# Supply chain/ops/outsider/director flags
boardex_raw['SCM_flag'] = np.where(boardex_raw['RoleName'].str.contains('|'.join(manager_string_SCM)), 1, 0)
boardex_raw['director_flag'] = np.where(~boardex_raw['BrdPosition'].str.contains('No'), 1, 0)
boardex_raw['COO_flag'] = np.where(boardex_raw['RoleName'].str.contains('|'.join(manager_string_COO)), 1, 0)
boardex_raw['independent_director'] = np.where(boardex_raw['NED'].str.contains('Yes'), 1, 0) #NED or non-excutive director is interchanable for independende director and very accurate

# arrange by company, director, position, date in order to determine for each manager its start and end date
boardex_raw = boardex_raw.sort_values(['CompanyID', 'DirectorID', 'RoleName', 'DateStartRole'])

# extract date of start and date of end
boardex_final = boardex_raw[~boardex_raw.DateStartRole.str.contains("N")]  # delete rows with N in them instead of date, note that this is done at the end, so as not to mistaknly give CEO not a real date.
boardex_final['DateStartRole'] = pd.to_datetime(boardex_final['DateStartRole'])  # convert to date format
boardex_final = boardex_final.dropna(subset=['DateEndRole'])
boardex_final = boardex_final[~boardex_final.DateEndRole.str.contains("N")]  # delete rows with N in them instead of date, note that this is done at the end, so as not to mistaknly give CEO not a real date.
boardex_final['DateEndRole'] = pd.to_datetime(boardex_final['DateEndRole'])  # convert to date format

boardex_final['StartYear'] = boardex_final['DateStartRole'].dt.year  # extract start year
boardex_final['EndYear'] = boardex_final['DateEndRole'].dt.year  # extract end year
# df['month_year'] = df['date_column'].dt.strftime('%B-%Y')

boardex_final = boardex_final[(boardex_final['EndYear'] >= 1997) & (
(boardex_final['StartYear'] < 2020))]  # make sure was in role during 1997, and drop all that started in 2020

boardex_final = boardex_final[
    ['CompanyID', 'CompanyName', 'CIKCode', 'Ticker', 'DirectorID', 'RoleName', 'DateStartRole', 'DateEndRole',
     'Gender', 'Nationality','FirstWorkCountry','NationalityORG', 'NetworkSize','PriorExperience', 'SCM_flag', 'director_flag','independent_director', 'COO_flag', 'StartYear',
     'EndYear','CumulativeSectors']]  # final columns

#drop redundand db
del boardex_raw ,boardex_raw_company ,boardex_raw_individual_profile ,boardex_raw_manager ,boardex_raw_manager_company

#transform the data to be based on company-date, so you will have one row per firm per date and have all the data you need

##create a time series
# generate a key for a row, for a specific relationship
boardex_final_company=boardex_final.reset_index()  # need this to generate an index per relationship for the next step
del boardex_final #makes things more efficent
boardex_date = boardex_final_company[['index', 'DateStartRole', 'DateEndRole']]
# added months between date_from and date_to
boardex_date = pd.concat([pd.Series(r.index, pd.date_range(r.DateStartRole, r.DateEndRole, freq='M'))
                          for r in boardex_date.itertuples()]).reset_index()
boardex_date.columns = ['job_date', 'index']
# merge back to database, based on the index I created.
boardex_final_company = pd.merge(boardex_final_company, boardex_date)
boardex_final_company['job_date_quarter'] = pd.PeriodIndex(pd.to_datetime(boardex_final_company.job_date), freq='Q')  # generate quarter

del boardex_date

#Keep last montho old job (usually ends in the end anyways), and if there are multiple jobs, keep record of newst one
boardex_final_company.drop_duplicates(subset=['CompanyID','DirectorID','job_date'],keep='first',inplace=True)

#indicator for the presence of a foriegn national in general, in order to had to the regression as a control
boardex_final_company['foreign_national_indicator'] = np.where((boardex_final_company.Nationality.isna()) | (boardex_final_company['Nationality']=='American' ),0,1)
boardex_final_company['foreign_national_ind_ORG'] = np.where((boardex_final_company.NationalityORG.isna()) | (boardex_final_company['NationalityORG']=='American' ),0,1)

# fill na with string so the tranform-join work
boardex_final_company.Nationality = boardex_final_company.Nationality.fillna('M')
boardex_final_company.NationalityORG = boardex_final_company.NationalityORG.fillna('M')


#genereate nationality variables for each sub group we want #all, all directors, insiders, outsiders, and SCOM
#do this using an interim indicator 'nationality temp' which takes the nationality of the sub group alone
# and then sum the nationality of only that sub group

#nationality of just all directors
boardex_final_company['Nationality_directors'] = np.where(boardex_final_company['director_flag']==1,boardex_final_company.Nationality,'M')
boardex_final_company['Nationality_directors']=boardex_final_company.groupby(['CompanyID','job_date'])['Nationality_directors'].transform(lambda x: ''.join(set(x)))
#nationality of all insiders (i.e. not outside directors - includes both directors and inside managment that boardex reports)
boardex_final_company['Nationality_insiders'] = np.where(boardex_final_company['independent_director']==0,boardex_final_company.Nationality,'M')
boardex_final_company['Nationality_insiders']=boardex_final_company.groupby(['CompanyID','job_date'])['Nationality_insiders'].transform(lambda x: ''.join(set(x)))
#nationality of Inside Directors
boardex_final_company['Nationality_inside_directors'] = np.where((boardex_final_company['independent_director']==0) & (boardex_final_company['director_flag']==1),boardex_final_company.Nationality,'M')
boardex_final_company['Nationality_inside_directors']=boardex_final_company.groupby(['CompanyID','job_date'])['Nationality_inside_directors'].transform(lambda x: ''.join(set(x)))
#nationality of all outsiders
boardex_final_company['Nationality_outsiders'] = np.where(boardex_final_company['independent_director']==1,boardex_final_company.Nationality,'M')
boardex_final_company['Nationality_outsiders']=boardex_final_company.groupby(['CompanyID','job_date'])['Nationality_outsiders'].transform(lambda x: ''.join(set(x)))
#nationality of SCOM
boardex_final_company['Nationality_scom'] = np.where((boardex_final_company['COO_flag']==1) | (boardex_final_company['SCM_flag']==1) ,boardex_final_company.Nationality,'M')
boardex_final_company['Nationality_scom']=boardex_final_company.groupby(['CompanyID','job_date'])['Nationality_scom'].transform(lambda x: ''.join(set(x)))
#nationality of COO
boardex_final_company['Nationality_coo'] = np.where((boardex_final_company['COO_flag']==1),boardex_final_company.Nationality,'M')
boardex_final_company['Nationality_coo']=boardex_final_company.groupby(['CompanyID','job_date'])['Nationality_coo'].transform(lambda x: ''.join(set(x)))
#nationality of SCM
boardex_final_company['Nationality_scm'] = np.where((boardex_final_company['SCM_flag']==1),boardex_final_company.Nationality,'M')
boardex_final_company['Nationality_scm']=boardex_final_company.groupby(['CompanyID','job_date'])['Nationality_scm'].transform(lambda x: ''.join(set(x)))
#nationality of all boardex universe #do this at the end, else it will join for everyone unneceserally
boardex_final_company['Nationality']=boardex_final_company.groupby(['CompanyID','job_date'])['Nationality'].transform(lambda x: ''.join(set(x)))
boardex_final_company['NationalityORG']=boardex_final_company.groupby(['CompanyID','job_date'])['NationalityORG'].transform(lambda x: ''.join(set(x)))

#generate indicator for foreign direcotrs/outsider/insider
boardex_final_company['foreign_director_indicator'] = boardex_final_company['director_flag']*boardex_final_company['foreign_national_indicator']
boardex_final_company['foreign_director_ORG_indicator'] = boardex_final_company['director_flag']*boardex_final_company['foreign_national_ind_ORG']
boardex_final_company['foreign_outsider_indicator'] = (boardex_final_company['independent_director'])*boardex_final_company['foreign_national_indicator']
boardex_final_company['foreign_outsider_ORG_indicator'] = (boardex_final_company['independent_director'])*boardex_final_company['foreign_national_ind_ORG']
boardex_final_company['foreign_insider_indicator'] = np.where((boardex_final_company['foreign_national_indicator']==1)&(boardex_final_company['director_flag']==0),1,0)
boardex_final_company['foreign_insider_ORG_indicator'] = np.where((boardex_final_company['foreign_national_ind_ORG']==1)&(boardex_final_company['director_flag']==0),1,0)
boardex_final_company['foreign_national_experienced'] = boardex_final_company['PriorExperience']*boardex_final_company['foreign_national_indicator']
boardex_final_company['domestic_experienced'] = boardex_final_company['PriorExperience'] * (1 - boardex_final_company['foreign_national_indicator'])

#save director level data - HUGE
boardex_final_company.to_csv('boardex_director_Level_nationality.csv',index=False)

#generatre firm level variables
boardex_final_company['director_flag']=boardex_final_company.groupby(['CompanyID','job_date'])['director_flag'].transform("sum")
boardex_final_company['COO_flag']=boardex_final_company.groupby(['CompanyID','job_date'])['COO_flag'].transform("sum")
boardex_final_company['SCM_flag']=boardex_final_company.groupby(['CompanyID','job_date'])['SCM_flag'].transform("sum")
boardex_final_company['independent_director']=boardex_final_company.groupby(['CompanyID','job_date'])['independent_director'].transform("sum")
boardex_final_company['foreign_national_sum']=boardex_final_company.groupby(['CompanyID','job_date'])['foreign_national_indicator'].transform("sum")
boardex_final_company['foreign_national_indicator']=boardex_final_company.groupby(['CompanyID','job_date'])['foreign_national_indicator'].transform("max")
boardex_final_company['foreign_national_ind_ORG_sum']=boardex_final_company.groupby(['CompanyID','job_date'])['foreign_national_ind_ORG'].transform("sum")
boardex_final_company['foreign_national_ind_ORG']=boardex_final_company.groupby(['CompanyID','job_date'])['foreign_national_ind_ORG'].transform("max")
#new added 2024-04-16
boardex_final_company['foreign_director_sum']=boardex_final_company.groupby(['CompanyID','job_date'])['foreign_director_indicator'].transform("sum")
boardex_final_company['foreign_director_indicator']=boardex_final_company.groupby(['CompanyID','job_date'])['foreign_director_indicator'].transform("max")
boardex_final_company['foreign_director_ORG_sum']=boardex_final_company.groupby(['CompanyID','job_date'])['foreign_director_ORG_indicator'].transform("sum")
boardex_final_company['foreign_director_ORG_indicator']=boardex_final_company.groupby(['CompanyID','job_date'])['foreign_director_ORG_indicator'].transform("max")
boardex_final_company['foreign_outsider_sum']=boardex_final_company.groupby(['CompanyID','job_date'])['foreign_outsider_indicator'].transform("sum")
boardex_final_company['foreign_outsider_indicator']=boardex_final_company.groupby(['CompanyID','job_date'])['foreign_outsider_indicator'].transform("max")
boardex_final_company['foreign_outsider_ORG_sum']=boardex_final_company.groupby(['CompanyID','job_date'])['foreign_outsider_ORG_indicator'].transform("sum")
boardex_final_company['foreign_outsider_ORG_indicator']=boardex_final_company.groupby(['CompanyID','job_date'])['foreign_outsider_ORG_indicator'].transform("max")
boardex_final_company['foreign_insider_sum']=boardex_final_company.groupby(['CompanyID','job_date'])['foreign_insider_indicator'].transform("sum")
boardex_final_company['foreign_insider_indicator']=boardex_final_company.groupby(['CompanyID','job_date'])['foreign_insider_indicator'].transform("max")
boardex_final_company['foreign_insider_ORG_sum']=boardex_final_company.groupby(['CompanyID','job_date'])['foreign_insider_ORG_indicator'].transform("sum")
boardex_final_company['foreign_insider_ORG_indicator']=boardex_final_company.groupby(['CompanyID','job_date'])['foreign_insider_ORG_indicator'].transform("max")
boardex_final_company['foreign_national_experienced_sum']=boardex_final_company.groupby(['CompanyID','job_date'])['foreign_national_experienced'].transform("sum")
boardex_final_company['foreign_national_experienced']=boardex_final_company.groupby(['CompanyID','job_date'])['foreign_national_experienced'].transform("max")
boardex_final_company['domestic_experienced_sum']=boardex_final_company.groupby(['CompanyID','job_date'])['domestic_experienced'].transform("sum")
boardex_final_company['domestic_experienced']=boardex_final_company.groupby(['CompanyID','job_date'])['domestic_experienced'].transform("max")
boardex_final_company['Total_director_managers']=boardex_final_company.groupby(['CompanyID','job_date'])["DirectorID"].transform("count")


#drop redundand at this stage, otherwise database too large
boardex_final_company.drop_duplicates(subset=['CompanyID','job_date'],inplace=True)
boardex_final_company.drop(columns=['index','StartYear','EndYear','DateStartRole','DateEndRole','Gender','RoleName','DirectorID','NetworkSize',
                                    'FirstWorkCountry',"PriorExperience",'CumulativeSectors'],inplace=True)

###Generate Industry of Directors foreigner and Domestic###
#Merge all industry that foreign nationals were in. You have to load the data, otherwise it just collapses
import pandas as pd
# Load the data with specified columns and parse the job_date
boardex_director_sector = pd.read_csv('boardex_director_Level_nationality.csv',usecols=['CompanyID', 'job_date', 'CumulativeSectors', 'foreign_national_indicator'],
    parse_dates=['job_date'])
# Function to convert entries in 'CumulativeSectors' to sets, assuming input is correctly formatted
def convert_to_set(x):
    if isinstance(x, str):
        try:
            return set(eval(x))
        except:
            return set()
    elif isinstance(x, list):
        return set(x)
    elif isinstance(x, set):
        return x
    else:
        return {x}
# Apply the conversion function
boardex_director_sector['CumulativeSectors'] = boardex_director_sector['CumulativeSectors'].apply(convert_to_set)
# Define an aggregation function that flattens all items into a single set per group
def aggregate_sectors(sets):
    result_set = set()
    for sector_set in sets:
        result_set.update(sector_set)
    return result_set
# Aggregate data for foreign nationals
foreign_aggregated = boardex_director_sector[boardex_director_sector['foreign_national_indicator'] == 1].groupby(['CompanyID', 'job_date']).agg({
    'CumulativeSectors': aggregate_sectors
}).reset_index()
foreign_aggregated.rename(columns={'CumulativeSectors': 'AggregatedSectorsForeign'}, inplace=True)
# Aggregate data for non-foreign nationals
domestic_aggregated = boardex_director_sector[boardex_director_sector['foreign_national_indicator'] == 0].groupby(['CompanyID', 'job_date']).agg({
    'CumulativeSectors': aggregate_sectors
}).reset_index()
domestic_aggregated.rename(columns={'CumulativeSectors': 'AggregatedSectorsDomestic'}, inplace=True)
# Merge the foreign and domestic results
merged_aggregated_sectors = pd.merge(foreign_aggregated, domestic_aggregated, on=['CompanyID', 'job_date'], how='outer')
merged_aggregated_sectors.to_csv('data/working_files/boardex_director_sector_aggregated_v5_20240508.csv', index=False)

del boardex_director_sector, foreign_aggregated, domestic_aggregated

#merge to main data
boardex_final_company = pd.merge(boardex_final_company, merged_aggregated_sectors, on=['CompanyID', 'job_date'], how='left')

boardex_final_company.to_csv('boardex_final_company_SCM_director_nationality.csv')