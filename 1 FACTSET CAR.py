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


pd.set_option('display.expand_frame_repr', False)
# pd.set_option('display.max_colwidth', -1)
# Reduce decimal points to 2
pd.options.display.float_format = '{:,.4f}'.format

#set my desired working directory
os.chdir()

#in this file I would we set up the factaset data, to show  both suppliers and customers, as they are defined as "target" in Factset.

#load factset and cusip/ticker #need cusip to match to GVKEY
factset_id_cusip=pd.read_csv('entity_id_cusip_.csv')
factset_id_ticker=pd.read_csv('entity_id_ticker.csv')
gvkey_cc=pd.read_csv('gvkey_cusip_cik.csv',usecols=['gvkey','cusip','tic','cik'])###
gvkey_cc.drop_duplicates(subset=['gvkey','cusip','tic','cik'],inplace=True)###
#merge the two, but let it be based on the CUSIP
factset_id_cusip_ticker=pd.merge(factset_id_cusip,factset_id_ticker,how='left',on=['factset_entity_id'])
factset_id_cusip_ticker_gvkey_cik=pd.merge(factset_id_cusip_ticker,gvkey_cc,how='left',on=['cusip']) ###

factset_id_cusip_ticker_gvkey_cik.dropna(subset=['gvkey'],inplace=True) ###

#load non_us_supplier_to_us_firm, which means that the target is the supplier to the source
factset_target_suppliers=pd.read_csv('PC/non_us_supplier_to_us_firm.csv')
#add gvkey and cik
factset_target_suppliers=pd.merge(factset_target_suppliers,factset_id_cusip_ticker_gvkey_cik,how='left',left_on=['source_factset_entity_id'],right_on=['factset_entity_id'])
factset_target_suppliers=factset_target_suppliers.drop(columns=['factset_entity_id','cusip','ticker_region'])#delete redunand
factset_target_suppliers['customer_reports']=1

#change the name of the coulmns to a workable name
factset_target_suppliers.rename(columns={'source_factset_entity_id': 'customer_factset_entity_id', 'source_entity_name':'customer_name' , 'source_entity_country':'customer_country', 'source_entity_type':'customer_type',
                   'target_factset_entity_id':'supplier_factset_entity_id','target_entity_name':'supplier_name','target_country':'supplier_country','target_entity_type':'supplier_type','cusip':'customer_cusip','ticker_region':'customer_ticker_region'},inplace=True)

#load us_customer_to_non_us_firm, which means that the target is the supplier to the source
factset_target_customers=pd.read_csv('us_customer_to_non_us_firm.csv')
#add Ticker and CUSIP to US firms
factset_target_customers=pd.merge(factset_target_customers,factset_id_cusip_ticker_gvkey_cik,how='left',left_on=['target_factset_entity_id'],right_on=['factset_entity_id'])
factset_target_customers=factset_target_customers.drop(columns=['factset_entity_id','cusip','ticker_region']) #delete redunand
factset_target_customers['customer_reports']=0

#change the name of the coulmns to a workable name
factset_target_customers.rename(columns={'source_factset_entity_id': 'supplier_factset_entity_id', 'source_entity_name':'supplier_name' , 'source_entity_country':'supplier_country', 'source_entity_type':'supplier_type',
                   'target_factset_entity_id':'customer_factset_entity_id','target_entity_name':'customer_name','target_country':'customer_country','target_entity_type':'customer_type','cusip':'customer_cusip','ticker_region':'customer_ticker_region'},inplace=True)

#stack the two databases together, generates one database to see the WHOLE universe of Suppliers.
factset_raw=pd.concat([factset_target_suppliers,factset_target_customers],sort=True)

#drop duplicates between the two datebased #todo talk to Rohan to make sure you do not drop too many.
factset_raw.drop_duplicates(subset=['customer_factset_entity_id','relationship_end_date','relationship_start_date','supplier_factset_entity_id'],keep='first',inplace=True)

##create a time series for the relationship start and end date## (if I want the first full month as well, change '%Y%m%d' to '%Y%m'

#convert to date object
factset_raw['relationship_start_date']=pd.to_datetime(factset_raw['relationship_start_date']).dt.strftime('%Y%m%d')
factset_raw['relationship_end_date']=pd.to_datetime(factset_raw['relationship_end_date']).dt.strftime('%Y%m%d')

#fill NA with end of August
factset_raw['relationship_end_date'].replace({'NaT': '20200831'}, inplace=True)
factset_raw['relationship_end_date'].fillna('20200831',inplace=True)

#convert to date datetime
factset_raw['relationship_start_date']=pd.to_datetime(factset_raw['relationship_start_date'], format='%Y%m%d')
factset_raw['relationship_end_date']=pd.to_datetime(factset_raw['relationship_end_date'], format='%Y%m%d')

#generate a key for a row, for a specific relationship
factset_raw.reset_index(inplace=True) #need this to generate an index per relationship for the next step
factset_date=factset_raw[['index','relationship_start_date','relationship_end_date']]
#added months between date_from and date_to
factset_date = pd.concat([pd.Series(r.index, pd.date_range(r.relationship_start_date, r.relationship_end_date, freq='M'))
                 for r in factset_date.itertuples()]).reset_index()
factset_date.columns = ['relationship_date', 'index']
#merge back to database, based on the index I created.
factset=pd.merge(factset_raw,factset_date)

del factset_date, factset_target_suppliers, factset_target_customers, factset_id_cusip,factset_id_ticker

#drop those observations where there are overlapping months
factset.drop_duplicates(subset=['customer_factset_entity_id','supplier_factset_entity_id','relationship_date'],inplace=True)
factset.drop(columns='index',inplace=True)
factset.dropna(subset=['gvkey'],inplace=True) #drop missing gvkey

#calculate quarter date
factset['relationship_qtr'] = pd.PeriodIndex(pd.to_datetime(factset.relationship_date), freq='Q')

##Industry of Supplier##
# Factset provides a file with which you can merge supplier Entity ID with SIC code,  or you can skip,
# then this code matches them with Boardex industry mapping, becuase boardex does not directly provide SIC Code,
# You can also do this via SIC code from COMPUSTAT and the like becuase we merged GVKEY and CUSIP to boardex data
factset_indstry = pd.read_excel("",usecols=['Entity ID','FactSet Ind',"SIC Code"]).dropna(subset=['FactSet Ind'])
factset_indstry.rename(columns={'Entity ID':'supplier_factset_entity_id','FactSet Ind':'supplier_industry',"SIC Code":"sic_factset"},inplace=True)
factset_indstry = factset_indstry[factset_indstry['supplier_industry']!="@NA"]
factset_indstry = factset_indstry[factset_indstry['supplier_factset_entity_id']!="@NA"]
factset_indstry = factset_indstry.drop_duplicates(subset=['supplier_factset_entity_id'],keep='last')

# Define the mapping between List 1 and List 2 items
mapping = {
    'Real Estate Development': 'Real Estate',
    'Hotels/Resorts/Cruiselines': 'Leisure & Hotels',
    'Investment Trusts/Mutual Funds': 'Investment Companies',
    'Internet Software/Services': 'Software & Computer Services',
    'Packaged Software': 'Software & Computer Services',
    'Precious Metals': 'Mining',
    'Information Technology Services': 'Software & Computer Services',
    'Other Metals/Minerals': 'Mining',
    'Oil & Gas Production': 'Oil & Gas',
    'Electrical Products': 'Electronic & Electrical Equipment',
    'Apparel/Footwear': 'Clothing & Personal Products',
    'Movies/Entertainment': 'Media & Entertainment',
    'Specialty Telecommunications': 'Telecommunication Services',
    'Specialty Stores': 'General Retailers',
    'Publishing: Books/Magazines': 'Publishing',
    'Internet Retail': 'General Retailers',
    'Home Furnishings': 'Household Products',
    'Miscellaneous Commercial Services': 'Business Services',
    'Auto Parts: OEM': 'Automobiles & Parts',
    'Financial Conglomerates': 'Speciality & Other Finance',
    'Biotechnology': 'Pharmaceuticals and Biotechnology',
    'Commercial Printing/Forms': 'Publishing',
    'Chemicals: Specialty': 'Chemicals',
    'Wholesale Distributors': 'Wholesale Trade',
    'Medical Distributors': 'Health',
    'Computer Communications': 'Telecommunication Services',
    'Food: Major Diversified': 'Food Producers & Processors',
    'Medical/Nursing Services': 'Health',
    'Miscellaneous': 'Unknown',
    'Engineering & Construction': 'Construction & Building Materials',
    'Food: Specialty/Candy': 'Food Producers & Processors',
    'Investment Banks/Brokers': 'Speciality & Other Finance',
    'Building Products': 'Construction & Building Materials',
    'Personnel Services': 'Business Services',
    'Broadcasting': 'Media & Entertainment',
    'Investment Managers': 'Investment Companies',
    'Regional Banks': 'Banks',
    'Savings Banks': 'Banks',
    'Restaurants': 'Leisure & Hotels',
    'Major Banks': 'Banks',
    'Medical Specialties': 'Health',
    'Finance/Rental/Leasing': 'Speciality & Other Finance',
    'Integrated Oil': 'Oil & Gas',
    'Airlines': 'Transport',
    'Real Estate Investment Trusts': 'Real Estate',
    'Other Transportation': 'Transport',
    'Electronic Equipment/Instruments': 'Electronic & Electrical Equipment',
    'Marine Shipping': 'Transport',
    'Multi-Line Insurance': 'Insurance',
    'Miscellaneous Manufacturing': 'Diversified Industrials',
    'Construction Materials': 'Construction & Building Materials',
    'Advertising/Marketing Services': 'Business Services',
    'Homebuilding': 'Construction & Building Materials',
    'Computer Processing Hardware': 'Information Technology Hardware',
    'Electric Utilities': 'Electricity',
    'Steel': 'Steel & Other Metals',
    'Pharmaceuticals: Major': 'Pharmaceuticals and Biotechnology',
    'Insurance Brokers/Services': 'Insurance',
    'Oilfield Services/Equipment': 'Oil & Gas',
    'Trucks/Construction/Farm Machinery': 'Engineering & Machinery',
    'Media Conglomerates': 'Media & Entertainment',
    'Industrial Machinery': 'Engineering & Machinery',
    'Wireless Telecommunications': 'Telecommunication Services',
    'Casinos/Gaming': 'Leisure Goods',
    'Environmental Services': 'Business Services',
    'Chemicals: Agricultural': 'Chemicals',
    'Electronics/Appliances': 'Household Products',
    'Computer Peripherals': 'Information Technology Hardware',
    'Metal Fabrication': 'Engineering & Machinery',
    'Coal': 'Mining',
    'Industrial Conglomerates': 'Diversified Industrials',
    'Telecommunications Equipment': 'Telecommunication Services',
    'Electronic Production Equipment': 'Electronic & Electrical Equipment',
    'Alternative Power Generation': 'Renewable Energy',
    'Semiconductors': 'Electronic & Electrical Equipment',
    'Air Freight/Couriers': 'Transport',
    'Food: Meat/Fish/Dairy': 'Food Producers & Processors',
    'Household/Personal Care': 'Household Products',
    'Recreational Products': 'Leisure Goods',
    'Apparel/Footwear Retail': 'Clothing & Personal Products',
    'Food Distributors': 'Food & Drug Retailers',
    'Other Consumer Services': 'Consumer Services',
    'Trucking': 'Transport',
    'Catalog/Specialty Distribution': 'Wholesale Trade',
    'Food Retail': 'Food & Drug Retailers',
    'Major Telecommunications': 'Telecommunication Services',
    'Data Processing Services': 'Software & Computer Services',
    'Discount Stores': 'General Retailers',
    'Beverages: Non-Alcoholic': 'Beverages',
    'Textiles': 'Clothing & Personal Products',
    'Industrial Specialties': 'Diversified Industrials',
    'Electronics Distributors': 'Wholesale Trade',
    'Aerospace & Defense': 'Aerospace & Defence',
    'Other Consumer Specialties': 'Consumer Services',
    'Drugstore Chains': 'Food & Drug Retailers',
    'Agricultural Commodities/Milling': 'Food Producers & Processors',
    'Publishing: Newspapers': 'Publishing',
    'Aluminum': 'Steel & Other Metals',
    'Automotive Aftermarket': 'Automobiles & Parts',
    'Containers/Packaging': 'Containers & Packaging',
    'Pulp & Paper': 'Forestry & Paper',
    'Motor Vehicles': 'Automobiles & Parts',
    'Forest Products': 'Forestry & Paper',
    'Chemicals: Major Diversified': 'Chemicals',
    'Hospital/Nursing Management': 'Health',
    'Electronic Components': 'Electronic & Electrical Equipment',
    'Contract Drilling': 'Oil & Gas',
    'Pharmaceuticals: Other': 'Pharmaceuticals and Biotechnology',
    'Oil Refining/Marketing': 'Oil & Gas',
    'Beverages: Alcoholic': 'Beverages',
    'Life/Health Insurance': 'Life Assurance',
    'Services to the Health Industry': 'Health',
    'Office Equipment/Supplies': 'Business Services',
    'Pharmaceuticals: Generic': 'Pharmaceuticals and Biotechnology',
    'Water Utilities': 'Utilities - Other',
    'Specialty Insurance': 'Insurance',
    'Tools & Hardware': 'Diversified Industrials',
    'Cable/Satellite TV': 'Media & Entertainment',
    'Gas Distributors': 'Utilities - Other',
    'Electronics/Appliance Stores': 'General Retailers',
    'Property/Casualty Insurance': 'Insurance',
    'Financial Publishing/Services': 'Business Services',
    'Oil & Gas Pipelines': 'Oil & Gas',
    'Department Stores': 'General Retailers',
    'Managed Health Care': 'Health',
    'Tobacco': 'Tobacco',
    'Railroads': 'Transport',
    'Home Improvement Chains': 'General Retailers',
    'Consumer Sundries': 'Household Products'
}
# Replace items
factset_indstry['mapped_sup_industry'] = factset_indstry['supplier_industry'].map(mapping)


factset.to_csv('Analysis/data/working_files/factset_time_series_20240507.csv')