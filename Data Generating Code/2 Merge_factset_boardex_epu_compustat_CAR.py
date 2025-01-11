import pandas as pd
import re
import numpy as np
import math
from datetime import datetime, timedelta,date
import os
import time
import glob
from scipy.stats import mstats
from datequarter import DateQuarter
import ast


pd.set_option('display.expand_frame_repr', False)
#pd.set_option('display.max_colwidth', -1)
# Reduce decimal points to 2
pd.options.display.float_format = '{:,.4f}'.format
pd.set_option('display.max_rows', 500)

#set my desired working directory
os.chdir()

factset=pd.read_csv('factset_time_series.csv')

boardex=pd.read_csv('boardex_final_company_SCM_director_nationality.csv')

epu=pd.read_csv('epu_time_series.csv')

#all epu - Factset countries
epu_factset_country=['Australia', 'Belgium', 'Brazil', 'Canada', 'Chile', 'China',
       'Colombia', 'Croatia', 'France',
       'Germany', 'Greece', 'India', 'Ireland', 'Italy',
       'Japan', 'Korea', 'Mainland China', 'Mexico',
       'Netherlands', 'Pakistan', 'Russia', 'Singapore',
       'Spain', 'Sweden', 'UK']

#replace taiwan and Russian Federation to match EPU
factset['supplier_country']=factset['supplier_country'].replace({'Taiwan':'China',"Russian Federation":"Russia","United Kingdom":"UK","South Korea":"Korea"},regex=True)

#keep only suppliers from countries with EPU score
factset = factset[(factset['supplier_country'].str.contains('|'.join(epu_factset_country)))]

#drop reapting quarters
factset.drop_duplicates(subset=['customer_factset_entity_id','supplier_factset_entity_id','relationship_qtr'],inplace=True)

##at this stage we have Factset of EPU countries only##
#calculate past EPU shocks
for i in ["high_epu_mean_diff_10th_indicator"]:
    epu[i+"Q_1"] = epu.groupby(['country'])[i].shift(1)#past
    epu[i+"Q_2"] = epu.groupby(['country'])[i].shift(2)
    epu[i+"Q_3"] = epu.groupby(['country'])[i].shift(3)
    epu[i+"Q_4"] = epu.groupby(['country'])[i].shift(4)
    epu[i+"Q1"] = epu.groupby(['country'])[i].shift(-1)#future
    epu[i+"Q2"] = epu.groupby(['country'])[i].shift(-2)
    epu[i+"Q3"] = epu.groupby(['country'])[i].shift(-3)
    epu[i+"Q4"] = epu.groupby(['country'])[i].shift(-4)

#change nationality to country name
#all nationality that have EPU, from boardex and epu
epu_director=['Australian','British','Dutch','German','Italian','Belgian','Spanish',
 'Brazilian','Indian','Swedish','Canadian','Chilean','Chinese','Chinese (Taiwan)','French',
 'Colombian','Croatian','Greek','South Korean','Japanese','Irish','Mexican','Pakistani','Russian','Singaporean','American']

#transform monthly data to quarterly (add everythig to not miss a quarter)
boardex['COO_flag']=boardex.groupby(['CompanyID','job_date_quarter'])['COO_flag'].transform("max")
boardex['foreign_national_indicator']=boardex.groupby(['CompanyID','job_date_quarter'])['foreign_national_indicator'].transform("max")  #control for regeressions
boardex['foreign_national_ind_ORG']=boardex.groupby(['CompanyID','job_date_quarter'])['foreign_national_ind_ORG'].transform("max")
boardex['foreign_national_sum']=boardex.groupby(['CompanyID','job_date_quarter'])['foreign_national_sum'].transform("mean")
boardex['foreign_national_ind_ORG_sum']=boardex.groupby(['CompanyID','job_date_quarter'])['foreign_national_ind_ORG_sum'].transform("mean")
boardex['director_flag']=boardex.groupby(['CompanyID','job_date_quarter'])['director_flag'].transform("mean")
boardex['Total_director_managers']=boardex.groupby(['CompanyID','job_date_quarter'])['Total_director_managers'].transform("mean")
boardex['independent_director']=boardex.groupby(['CompanyID','job_date_quarter'])['independent_director'].transform("mean")
boardex['SCM_flag']=boardex.groupby(['CompanyID','job_date_quarter'])['SCM_flag'].transform("max")
boardex['Nationality_directors']=boardex.groupby(['CompanyID','job_date_quarter'])['Nationality_directors'].transform(lambda x: ''.join(set(x)))
boardex['Nationality_insiders']=boardex.groupby(['CompanyID','job_date_quarter'])['Nationality_insiders'].transform(lambda x: ''.join(set(x)))
boardex['Nationality_outsiders']=boardex.groupby(['CompanyID','job_date_quarter'])['Nationality_outsiders'].transform(lambda x: ''.join(set(x)))
boardex['Nationality_scom']=boardex.groupby(['CompanyID','job_date_quarter'])['Nationality_scom'].transform(lambda x: ''.join(set(x)))
boardex['Nationality_coo']=boardex.groupby(['CompanyID','job_date_quarter'])['Nationality_coo'].transform(lambda x: ''.join(set(x)))
boardex['Nationality_scm']=boardex.groupby(['CompanyID','job_date_quarter'])['Nationality_scm'].transform(lambda x: ''.join(set(x)))
boardex['NationalityORG']=boardex.groupby(['CompanyID','job_date_quarter'])['NationalityORG'].transform(lambda x: ''.join(set(x)))
boardex['Nationality']=boardex.groupby(['CompanyID','job_date_quarter'])['Nationality'].transform(lambda x: ''.join(set(x)))
boardex["foreign_director_sum"] = boardex.groupby(['CompanyID','job_date_quarter'])['foreign_director_sum'].transform("mean")
boardex["foreign_director_indicator"] = boardex.groupby(['CompanyID','job_date_quarter'])['foreign_director_indicator'].transform("max")
boardex["foreign_director_ORG_sum"] = boardex.groupby(['CompanyID','job_date_quarter'])['foreign_director_ORG_sum'].transform("mean")
boardex["foreign_director_ORG_indicator"] = boardex.groupby(['CompanyID','job_date_quarter'])['foreign_director_ORG_indicator'].transform("max")
boardex["foreign_outsider_sum"] = boardex.groupby(['CompanyID','job_date_quarter'])['foreign_outsider_sum'].transform("mean")
boardex["foreign_outsider_indicator"] = boardex.groupby(['CompanyID','job_date_quarter'])['foreign_outsider_indicator'].transform("max")
boardex["foreign_outsider_ORG_sum"] = boardex.groupby(['CompanyID','job_date_quarter'])['foreign_outsider_ORG_sum'].transform("mean")
boardex["foreign_outsider_ORG_indicator"] = boardex.groupby(['CompanyID','job_date_quarter'])['foreign_outsider_ORG_indicator'].transform("max")
boardex["foreign_insider_sum"] = boardex.groupby(['CompanyID','job_date_quarter'])['foreign_insider_sum'].transform("mean")
boardex["foreign_insider_indicator"] = boardex.groupby(['CompanyID','job_date_quarter'])['foreign_insider_indicator'].transform("max")
boardex["foreign_insider_ORG_sum"] = boardex.groupby(['CompanyID','job_date_quarter'])['foreign_insider_ORG_sum'].transform("mean")
boardex["foreign_insider_ORG_indicator"] = boardex.groupby(['CompanyID','job_date_quarter'])['foreign_insider_ORG_indicator'].transform("max")
boardex["foreign_national_experienced"] = boardex.groupby(['CompanyID','job_date_quarter'])['foreign_national_experienced'].transform("max")
boardex["foreign_national_experienced_sum"] = boardex.groupby(['CompanyID','job_date_quarter'])['foreign_national_experienced_sum'].transform("mean")

#Calculate foreign national sectors
# Define aggregation function for union of sets
def convert_to_set(item):
    if isinstance(item, str):  # Check if the item is a string
        try:
            # Convert string to set if it starts with '{'
            if item.startswith('{'):
                return ast.literal_eval(item)
        except (ValueError, SyntaxError):
            # Handle any conversion error
            return set()
    if isinstance(item, float) and pd.isna(item):  # Check if the item is a float and NaN
        return set()
    return set()
boardex['AggregatedSectorsDomestic'] = boardex['AggregatedSectorsDomestic'].apply(convert_to_set)
boardex['AggregatedSectorsForeign'] = boardex['AggregatedSectorsForeign'].apply(convert_to_set)
def union_sets(series):
    result_set = set()
    for item in series:
        if isinstance(item, set):
            result_set.update(item)
        else:
            print(f"Skipped non-set item: {item}")  # Optional: for debugging
    return result_set
# Apply transform with union_sets
# Perform aggregation to union the sets and create a temporary key to facilitate transform
domestic = boardex.groupby(['CompanyID', 'job_date_quarter']).agg({'AggregatedSectorsDomestic': union_sets}).reset_index()
foreign = boardex.groupby(['CompanyID', 'job_date_quarter']).agg({'AggregatedSectorsForeign': union_sets}).reset_index()
# Merge the aggregated sets back to the original DataFrame
print(len(boardex))
boardex = boardex.merge(domestic, on=['CompanyID', 'job_date_quarter'], suffixes=('', '_Q')) #Merge this back to the original DataFrame
boardex = boardex.merge(foreign, on=['CompanyID', 'job_date_quarter'], suffixes=('', '_Q')) #Merge this back to the original DataFrame
print(len(boardex))

del domestic, foreign

boardex.drop(columns=['AggregatedSectorsDomestic','AggregatedSectorsForeign'],inplace=True)

#drop repeating quarters
print(len(boardex))
boardex.drop_duplicates(subset=['CompanyID','job_date_quarter'],keep='last',inplace=True)
print(len(boardex))

#sum stats for directors for Boardex Data
boardex['job_date'] = boardex['job_date'].apply(lambda x: pd.Period(x, freq='d'))

#change nationalities to countries so you can merge easier
b_replace={'Australian':"Australia",'British':'UK','Dutch':"Netherlands",'German':"Germany",
           'Italian':'Italy','Belgian':'Belgium','Spanish':'Spain', 'Brazilian':'Brazil','Indian':"India",
           'Swedish':"Sweden",'Canadian':'Canada','Chilean':"Chile",'Chinese':"China",'Chinese (Taiwan)':"China",
           'French':"France",'Colombian':"Colombia",'Croatian':"Croatia",'Greek':"Greece",'South Korean':"Korea",
           'Japanese':"Japan",'Irish':"Ireland",'Mexican':"Mexico",'Pakistani':"Pakistan",'Russian':'Russia',
           'Singaporean':"Singapore",'American':"United Satates"}
boardex['Nationality']=boardex['Nationality'].replace(b_replace,regex=True)
boardex['NationalityORG']=boardex['NationalityORG'].replace(b_replace,regex=True)
boardex['Nationality_directors']=boardex['Nationality_directors'].replace(b_replace,regex=True)
boardex['Nationality_insiders']=boardex['Nationality_insiders'].replace(b_replace,regex=True)
boardex['Nationality_outsiders']=boardex['Nationality_outsiders'].replace(b_replace,regex=True)
boardex['Nationality_scom']=boardex['Nationality_scom'].replace(b_replace,regex=True)
boardex['Nationality_coo']=boardex['Nationality_coo'].replace(b_replace,regex=True)
boardex['Nationality_scm']=boardex['Nationality_scm'].replace(b_replace,regex=True)

#start merging data bases.
# start with factset and bordex, merge based on supplier_country and quarter
factset_boardex=pd.merge(left=factset,right=boardex,left_on=['cik','relationship_qtr'],right_on=['CIKCode','job_date_quarter'],how='left')

#then join factset_boardex with epu
fbu=pd.merge(left=factset_boardex,right=epu,left_on=['supplier_country','relationship_qtr'],right_on=['country','date_quarter'],how='left')

fbu.dropna(subset=['epu_min'],inplace=True) #drop missing epu score mainly 2020Q3 and countries with missing years such as belgium 2006
##at this stage I have the full database, clean it up a bit, and adjust some of the observations

#make sure director flag is now 1 if director from same country as the supplier, and otherwise 0.
fbu['foreign_national_flag']=np.where(fbu.apply(lambda x: str(x.country) in str(x.Nationality), axis=1),1,0) #flag if there is a foreign director from a supplier country
fbu['foreign_national_flag_ORG']=np.where(fbu.apply(lambda x: str(x.country) in str(x.NationalityORG), axis=1),1,0) #flag if there is a foreign director from a supplier country old version
fbu['foreign_directors_flag']=np.where(fbu.apply(lambda x: str(x.country) in str(x.Nationality_directors), axis=1),1,0) #flag if there is a foreign director from a supplier country
fbu['foreign_insiders_flag']=np.where(fbu.apply(lambda x: str(x.country) in str(x.Nationality_insiders), axis=1),1,0) #flag if there is a foreign director from a supplier country
fbu['foreign_outsiders_flag']=np.where(fbu.apply(lambda x: str(x.country) in str(x.Nationality_outsiders), axis=1),1,0) #flag if there is a foreign director from a supplier country
fbu['foreign_scom_flag']=np.where(fbu.apply(lambda x: str(x.country) in str(x.Nationality_scom), axis=1),1,0) #flag if there is a foreign director from a supplier country
fbu['foreign_coo_flag']=np.where(fbu.apply(lambda x: str(x.country) in str(x.Nationality_coo), axis=1),1,0) #flag if there is a foreign director from a supplier country
fbu['foreign_scm_flag']=np.where(fbu.apply(lambda x: str(x.country) in str(x.Nationality_scm), axis=1),1,0) #flag if there is a foreign director from a supplier country

#make sure the foreign national with experience is from a supplier country
fbu['foreign_national_exper_flag']= fbu['foreign_national_flag']*fbu['foreign_national_experienced']
fbu['foreign_national_exper_direct_flag']= fbu['foreign_directors_flag']*fbu['foreign_national_experienced']
fbu['foreign_national_exper_sum_flag']= fbu['foreign_national_flag']*fbu['foreign_national_experienced_sum']
fbu['foreign_national_experORG_flag']= fbu['foreign_national_flag_ORG']*fbu['foreign_national_experienced']
#examine whether the expert is from the supplier country
fbu['AggregatedSectorsForeign_Q'] = fbu['AggregatedSectorsForeign_Q'].apply(lambda x: x if isinstance(x, set) else set()) #adjust set
fbu['AggregatedSectorsDomestic_Q'] = fbu['AggregatedSectorsDomestic_Q'].apply(lambda x: x if isinstance(x, set) else set()) #adjust set

unique_combinations = fbu[['SectorInt', 'sic_factset']].dropna().drop_duplicates(subset=["SectorInt"]) #Create a DataFrame that contains unique combinations of SectorInt and factset_sic
mapping = dict(zip(unique_combinations['SectorInt'], unique_combinations['sic_factset']))# Step 2: Use this DataFrame to map SectorInt to factset_sic
def map_values(row, mapping,column):
    return {mapping.get(item, item) for item in row[column]}
fbu['MappedAggregatedSectorsForeign_Q'] = fbu.apply(lambda row: map_values(row, mapping,"AggregatedSectorsForeign_Q"), axis=1)
fbu['MappedAggregatedSectorsDomestic_Q'] = fbu.apply(lambda row: map_values(row, mapping,"AggregatedSectorsDomestic_Q"), axis=1)

fbu['foreign_national_supplier_expert'] = fbu.apply(lambda row: 1 if row['sic_factset'] in row['AggregatedSectorsForeign_Q'] else 0 , axis=1) #flag if there is a foreign director from a supplier country
fbu['domestic_national_supplier_expert_flag'] = fbu.apply(lambda row: 1 if row['sic_factset'] in row['AggregatedSectorsDomestic_Q'] else 0 , axis=1) #flag if there is a foreign director from a supplier country
fbu['foreign_national_supplier_expert_flag']= fbu['foreign_national_flag']*fbu['foreign_national_supplier_expert'] #flag if there is a foreign director from a supplier country who is a foriegn national

#make SCM_flag and COO_flag indicators
fbu['SCM_flag']=np.where(np.isnan(fbu['SCM_flag']),fbu['SCM_flag'],np.where(fbu['SCM_flag'] >0,1,0))
fbu['COO_flag']=np.where(np.isnan(fbu['COO_flag']),fbu['COO_flag'],np.where(fbu['COO_flag'] >0,1,0))

#drop missing companies
fbu.dropna(subset=['COO_flag'],inplace=True)

#clean some reduanded columns # I am dropping supplier information, since I am just looking for an indicator of whether that country has a supplier at that time, once I have all the infromation, the speicifc supplier details, are redundand
fbu.drop(columns=['Unnamed: 0_x','Unnamed: 0_y','CompanyID','CompanyName','CIKCode','Ticker','job_date','AggregatedSectorsForeign_Q','AggregatedSectorsDomestic_Q','job_date_quarter','Unnamed: 0','date_quarter','Nationality','NationalityORG','Nationality_directors', 'Nationality_insiders', 'Nationality_outsiders', 'Nationality_scom','Nationality_scm','Nationality_coo','relationship_end_date','relationship_start_date'],inplace=True)
del b_replace, boardex, epu, factset, factset_boardex

#save full data
fbu.to_csv('factset_boardex_epu_merged.csv')

#we now have customer-supplier-quarter relationship
##############################################################################################################################################
############################################ Run from here if you do not need to change anything #############################################
##############################################################################################################################################

## we now want to have unique data for regressions
#reload Data
fbu=pd.read_csv('factset_boardex_epu_merged.csv')

try:
    fbu.drop(columns=['supplier_name','supplier_type','relationship_type','Unnamed: 0'],inplace=True)
except:
    fbu.drop(columns=['supplier_name', 'supplier_type', 'relationship_type'], inplace=True)

#assume if you do not see SCM, director or COO flags, then it is 0.

##assume if you do not see SCM, director or COO flags, then drop firms, leave only boardex firms
fbu.dropna(subset=['SCM_flag', 'director_flag','COO_flag','foreign_national_flag','independent_director'],inplace=True)

#calculate number of suppliers per country
fbu['num_supplier_country_total']=fbu.groupby(['cik','country','relationship_qtr'])['cik'].transform('count')

#drop multiple observation per firm-quarter-country
fbu.drop_duplicates(subset=['gvkey','relationship_qtr','country'],inplace=True)

fbu.drop(columns=['supplier_factset_entity_id','customer_reports','customer_country','customer_type'],inplace=True)

fbu.sort_values(by=['gvkey',"country",'relationship_qtr'],inplace=True)

#convert to quarter date
fbu['relationship_qtr'] = pd.PeriodIndex(pd.to_datetime(fbu['relationship_date'].astype(str), format='%Y-%m-%d'), freq='Q')

##add lagged flags##
for i in ["foreign_national_flag","foreign_directors_flag","foreign_outsiders_flag","high_epu_mean_diff_10th_indicator"]:
    fbu[i+"Q_1"] = fbu.sort_values(by=['gvkey','country','relationship_qtr']).groupby(['gvkey','country'])[i].shift(1)#past
    fbu[i+"Q_2"] = fbu.sort_values(by=['gvkey','country','relationship_qtr']).groupby(['gvkey','country'])[i].shift(2)
    fbu[i+"Q_3"] = fbu.sort_values(by=['gvkey','country','relationship_qtr']).groupby(['gvkey','country'])[i].shift(3)
    fbu[i+"Q_4"] = fbu.sort_values(by=['gvkey','country','relationship_qtr']).groupby(['gvkey','country'])[i].shift(4)
    fbu[i+"Q1"] = fbu.sort_values(by=['gvkey','country','relationship_qtr']).groupby(['gvkey','country'])[i].shift(-1)#future
    fbu[i+"Q2"] = fbu.sort_values(by=['gvkey','country','relationship_qtr']).groupby(['gvkey','country'])[i].shift(-2)
    fbu[i+"Q3"] = fbu.sort_values(by=['gvkey','country','relationship_qtr']).groupby(['gvkey','country'])[i].shift(-3)
    fbu[i+"Q4"] = fbu.sort_values(by=['gvkey','country','relationship_qtr']).groupby(['gvkey','country'])[i].shift(-4)

#country cultural distance # if you have this calculation you can add it here, otherwise skip this part.
fbu["relationship_year"] = pd.to_datetime(fbu['relationship_date'], format='%Y-%m-%d').dt.year
cd = pd.read_excel('CulturalDifferenceUSA.xlsx',
                   usecols=['Country', 'Year','CulturalDistanceUSA'])
cd.rename(columns={'Country':'country','Year':'relationship_year'},inplace=True)
fbu=pd.merge(left=fbu,right=cd,on=['country','relationship_year'],how='left')
del cd

#country Common Law # if you have legal origin you can add here
cl = pd.read_excel()
cl.rename(columns={'Country':'country'},inplace=True)
fbu=pd.merge(left=fbu,right=cl,on=['country'],how='left')
del cl

#merge trade partner data from Census # if you have imports file from BEA, you can add it here.
trade = pd.read_excel("BEA.xlsx",usecols=["year","CTYNAME","IYR"])
rows = ["Africa","APEC","ASEAN (Association of Southeast Asian Nations)","Asia Near East","Asia - South","CAFTA-DR (Dominican Republic-Central America-United States Free Trade Agreement)","Central American Common Market",
        "Euro Area","Europe","European Union","LAFTA (Latin American Free Trade Area)","NATO (North Atlantic Treaty Organization) Allies","NICs (Newly Industrialized Countries)","North America",
        "OECD (Organization for Economic Cooperation and Development)","Pacific Rim Countries","South/Central America","Twenty Latin American Republics","World, Seasonally Adjusted","International Organizations",
        "Unidentified Countries","NAFTA with Mexico (Consump)","NAFTA with Canada (Consump)","Advanced Technology Products",'South and Central America', 'Pacific Rim', 'World, Not Seasonally Adjusted', 'Asia', 'CAFTA-DR',
        'Australia and Oceania', 'Sub Saharan Africa', 'USMCA with Mexico (Consump)', 'USMCA with Canada (Consump)','Falkland Islands(Islas Malvin']
trade = trade[~trade.CTYNAME.isin(rows)]
trade['CountryTradeRank'] = trade.groupby('year')['IYR'].rank(ascending=False)
trade = trade.sort_values(by=['year', 'CountryTradeRank'], ascending=[True, True])
#change name to fit merge
trade.rename(columns={'CTYNAME':'supplier_country','year':'relationship_year'},inplace=True) #change names to match merge file
trade["supplier_country"].replace({'United Kingdom':'UK','Korea, South':'Korea'},regex=True,inplace=True)
#adjust database for merge
fbu["relationship_year"] = fbu["relationship_qtr"].dt.year
#Merge
fbu = pd.merge(left=fbu,right=trade,on=['supplier_country','relationship_year'],how='left')

del trade, car, carq, treturn, mreturn

## add US EPU
epu=pd.read_csv('Analysis/data/working_files/epu_time_series_1987_2020.csv',parse_dates=['date_quarter'])
epu['relationship_qtr'] = pd.PeriodIndex(pd.to_datetime(epu['date_quarter'], format='%Y-%m-%d'), freq='Q')
epu = epu[epu["country"] == "US"]
epu.rename(columns={'epu_mean':'epu_mean_us',"epu_mean_diff":"epu_mean_diff_us","high_epu_mean_diff_10th_indicator":"EPU_SHOCK_US"},inplace=True)
fbu=pd.merge(left=fbu,right=epu[["relationship_qtr","EPU_SHOCK_US","epu_mean_diff_us",'epu_mean_us']],on=['relationship_qtr'],how='left')

#at this stage I have customer-country specific relationship

#####join Compustat from here

compustat=pd.read_csv('compustat quarterly.csv',parse_dates=['datadate'])

#convert to quarter data
compustat['datacqtr'] = pd.PeriodIndex(pd.to_datetime(compustat['datadate'].astype(str), format='%Y-%m-%d'), freq='Q')
#merge gdp data to compustat
compustat=pd.merge(left=compustat,right=gdp,on=['datacqtr'],how='left')

compustat.sort_values(by=['gvkey','datadate'],inplace=True)

#Various adjustments to the data in line with prior research
compustat = compustat[compustat["saleqw"]>10]
compustat = compustat[compustat["invtqw"]>1]
compustat = compustat[compustat["cogsqw"]>1]

#sum inventory in the next 4 quearter
compustat['invtqw_yearly'] = compustat.groupby('gvkey').apply(lambda x: x.rolling('365D', on='datadate', min_periods=4)['invtqw'].sum()).reset_index(0,drop=True)
compustat['invtqw_yearly']=  compustat.groupby('gvkey')['invtqw_yearly'].shift(-4)
compustat['invtqw_yearly_by_atg'] = compustat.invtqw_yearly/compustat.atqw

#sum revenue in the next 4 quearter
compustat['revtqw_yearly'] = compustat.groupby('gvkey').apply(lambda x: x.rolling('365D', on='datadate', min_periods=4)['revtqw'].sum()).reset_index(0,drop=True)
compustat['revtqw_yearly1']=  compustat.groupby('gvkey')['revtqw_yearly'].shift(-4)
compustat['revtqw_yearly_by_atg'] = compustat.revtqw_yearly/compustat.atqw

#sum cogs in the next 4 quearter
compustat['cogsqw_yearly'] = compustat.groupby('gvkey').apply(lambda x: x.rolling('365D', on='datadate', min_periods=4)['cogsqw'].sum()).reset_index(0,drop=True)
compustat['cogsqw_yearly']=  compustat.groupby('gvkey')['cogsqw_yearly'].shift(-4)
compustat['cogsqw_yearly_by_atg'] = compustat.cogsqw_yearly/compustat.atqw

#gross profit (rev-cogs)
compustat['gross_profit_yearly_by_atg'] = (compustat.revtqw_yearly - compustat.cogsqw_yearly)/compustat.atqw

#sum sale in the next 4 quearter
compustat['saleqw_yearly'] = compustat.groupby('gvkey').apply(lambda x: x.rolling('365D', on='datadate', min_periods=4)['saleqw'].sum()).reset_index(0,drop=True)
compustat['saleqw_yearly']=  compustat.groupby('gvkey')['saleqw_yearly'].shift(-4)
compustat['saleqw_yearly_by_atg'] = compustat.saleqw_yearly/compustat.atqw

#for ROE
#sum ibq in the next 4 quearter
compustat['ibq_yearly'] = compustat.groupby('gvkey').apply(lambda x: x.rolling('365D', on='datadate', min_periods=4)['ibq'].sum()).reset_index(0,drop=True)
compustat['ibq_yearly']=  compustat.groupby('gvkey')['ibq_yearly'].shift(-4)
#sum dvpq in the next 4 quearter
compustat['dvpq_yearly'] = compustat.groupby('gvkey').apply(lambda x: x.rolling('365D', on='datadate', min_periods=4)['dvpq'].sum()).reset_index(0,drop=True)
compustat['dvpq_yearly']=  compustat.groupby('gvkey')['dvpq_yearly'].shift(-4)
compustat['roe_yearly'] = (compustat.ibq_yearly - compustat.dvpq_yearly)/compustat.ceqq

#merge based on quarter date (same quarter as relationship and EPU score)
full_data=pd.merge(left=fbu,right=compustat,left_on=['gvkey','relationship_qtr'],right_on=['gvkey','datacqtr'],how='left')

full_data.dropna(subset=['atq'],inplace=True) #drop missing value #todo talk to Rohan to see if it is okay to drop this
print(full_data)

#save full company-date-country data
full_data.to_csv('Analysis/data/working_files/full_data.csv')

del full_data

#Now we have the full data. We are now going to collapse it to company-quarter level#
######################################################################################
#Novelty: use interaction between foreign national flag and high indicator to see if the foreign director is from a country with high indicator
#generate 𝐸𝑃𝑈 as an indicator variable equals one if at least one of the firm’s suppliers is located in a region where disruption in supply chain occurred in quarter t-1.
#use interaction between director flag and high indicator to see if the foreign director is from a country with high indicator
def foreign_times_high(df,epu_diff,foreign_flag):
    for i in epu_diff:
        for j in foreign_flag:
            df[str(j)+'*high_'+str(i)+'_10th_indicator']=df[str(j)]*fbu['high_'+str(i)+'_10th_indicator']
    return df

fbu=foreign_times_high(fbu,['epu_mean_diff'],['foreign_national_flag','foreign_national_flag_ORG',"foreign_national_flagQ_1","foreign_directors_flagQ_1",'foreign_directors_flag','foreign_insiders_flag','foreign_outsiders_flag','foreign_scom_flag','foreign_coo_flag','foreign_scm_flag'])

#todo: think what to do with directors and independante directors
# consider generating number of suppliers for disruption countries
#sum by gvkey-relationship, to get all the unique observations per the firm-quarter. In this case I will not have country, but rather cumulative data per the firm. Thus, Country fixed effects will be NOT needed.
epu_indicators= [
'high_epu_mean_diff_10th_indicator',
'foreign_national_flag*high_epu_mean_diff_10th_indicator',
'foreign_national_flag_ORG*high_epu_mean_diff_10th_indicator',
'foreign_national_flagQ_1*high_epu_mean_diff_10th_indicator',
'foreign_directors_flagQ_1*high_epu_mean_diff_10th_indicator',
'foreign_directors_flag*high_epu_mean_diff_10th_indicator',
'foreign_insiders_flag*high_epu_mean_diff_10th_indicator',
'foreign_outsiders_flag*high_epu_mean_diff_10th_indicator',
'foreign_scom_flag*high_epu_mean_diff_10th_indicator',
'foreign_scm_flag*high_epu_mean_diff_10th_indicator',
'foreign_coo_flag*high_epu_mean_diff_10th_indicator',
]

#Sum by gvkey-relationship, to get all the unique observations per the firm-quarter. In this case I will not have country, but rather cumulative data per the firm. Thus, Country fixed effects will be NOT needed.
fbu['high_epu_mean_diff_10th_sum']=fbu.groupby(['gvkey','relationship_qtr'])['high_epu_mean_diff_10th_indicator'].transform(sum) #added 20240521
fbu['foreign_national_sum']=fbu.groupby(['gvkey','relationship_qtr'])['foreign_national_flag'].transform(sum) #added 20240521
fbu['foreign_national_sum_t1']=fbu.groupby(['gvkey'])['foreign_national_sum'].shift(1) #added 20240521
fbu['foreign_national_flag*high_epu_mean_diff_10th_sum']=fbu.groupby(['gvkey','relationship_qtr'])['foreign_national_flag*high_epu_mean_diff_10th_indicator'].transform(sum) #added 20240521
fbu[epu_indicators]=fbu.groupby(['gvkey','relationship_qtr'])[epu_indicators].transform(max)
fbu['supplier_num_total']=fbu.groupby(['gvkey','relationship_qtr'])['num_supplier_country_total'].transform(sum)
fbu['num_supplier_countries']=fbu.groupby(['gvkey','relationship_qtr'])['supplier_country'].transform('count')
fbu['foreign_national_flag']=fbu.groupby(['gvkey','relationship_qtr'])['foreign_national_flag'].transform(max)
fbu['foreign_national_flag_ORG']=fbu.groupby(['gvkey','relationship_qtr'])['foreign_national_flag_ORG'].transform(max)
fbu['foreign_directors_flag']=fbu.groupby(['gvkey','relationship_qtr'])["foreign_directors_flag"].transform(max)
fbu['foreign_outsiders_flag']=fbu.groupby(['gvkey','relationship_qtr'])["foreign_outsiders_flag"].transform(max)
fbu['CulturalDistanceUSA']=fbu.groupby(['gvkey','relationship_qtr'])["CulturalDistanceUSA"].transform(np.mean)
fbu['CommonLaw']=fbu.groupby(['gvkey','relationship_qtr'])["CommonLaw"].transform(np.mean)
fbu["foreign_national_exper_flag"]=fbu.groupby(['gvkey','relationship_qtr'])["foreign_national_exper_flag"].transform(max)
fbu["foreign_national_experORG_flag"]=fbu.groupby(['gvkey','relationship_qtr'])["foreign_national_experORG_flag"].transform(max)
fbu["foreign_national_supplier_expert"]=fbu.groupby(['gvkey','relationship_qtr'])["foreign_national_supplier_expert"].transform(max)
fbu["domestic_national_supplier_expert_flag"]=fbu.groupby(['gvkey','relationship_qtr'])["domestic_national_supplier_expert_flag"].transform(max)
fbu["foreign_national_supplier_expert_flag"]=fbu.groupby(['gvkey','relationship_qtr'])["foreign_national_supplier_expert_flag"].transform(max)

#keep unique company/supplier data
fbu_company=fbu.drop_duplicates(subset=['gvkey','relationship_qtr'])

#merge based on quarter date (same quarter as epu and job)
full_data_company=pd.merge(left=fbu_company,right=compustat,left_on=['gvkey','relationship_qtr'],right_on=['gvkey','datacqtr'],how='left')
full_data_company.dropna(subset=['atq'],inplace=True) #drop missing value #todo talk to Rohan to see if it is okay to drop th

full_data_company.to_csv('Analysis/data/working_files/full_data_company_v9_20240410.csv')