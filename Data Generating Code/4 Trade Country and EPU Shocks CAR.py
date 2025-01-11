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

#load BEA Trade data by year. I only need the year, country and imports.
trade = pd.read_excel('BEATrade.xlsx',usecols=["year","CTYNAME","IYR"])
rows = ["Africa","APEC","ASEAN (Association of Southeast Asian Nations)","Asia Near East","Asia - South","CAFTA-DR (Dominican Republic-Central America-United States Free Trade Agreement)","Central American Common Market",
        "Euro Area","Europe","European Union","LAFTA (Latin American Free Trade Area)","NATO (North Atlantic Treaty Organization) Allies","NICs (Newly Industrialized Countries)","North America",
        "OECD (Organization for Economic Cooperation and Development)","Pacific Rim Countries","South/Central America","Twenty Latin American Republics","World, Seasonally Adjusted","International Organizations",
        "Unidentified Countries","NAFTA with Mexico (Consump)","NAFTA with Canada (Consump)","Advanced Technology Products",'South and Central America', 'Pacific Rim', 'World, Not Seasonally Adjusted', 'Asia', 'CAFTA-DR',
        'Australia and Oceania', 'Sub Saharan Africa', 'USMCA with Mexico (Consump)', 'USMCA with Canada (Consump)','Falkland Islands(Islas Malvin']
trade = trade[~trade.CTYNAME.isin(rows)]
trade['CountryTradeRank'] = trade.groupby('year')['IYR'].rank(ascending=False)
trade = trade.sort_values(by=['year', 'CountryTradeRank'], ascending=[True, True])
#change name to fit merge
trade.rename(columns={'CTYNAME':'country','year':'Year'},inplace=True) #change names to match merge file
trade["country"].replace({'United Kingdom':'UK','Korea, South':'Korea'},regex=True,inplace=True)

#changes in imports and ranks
trade['Import_change'] = trade.groupby('country')['IYR'].pct_change()
trade['CountryTradeRank_change'] = -trade.groupby('country')['CountryTradeRank'].diff()

#calculate EPU shocks
#load EPU data
epu_raw=pd.read_excel('EPU_Combined.xlsx')

#trransform to a panel
epu=epu_raw.melt(id_vars=['Year','Month'],
        var_name="country",
        value_name="epu_score").reset_index(drop=True)
#drop na
epu=epu.dropna(subset=['epu_score']).reset_index(drop=True)

#genereate mean EPU score per quarter
epu=epu.groupby(['country','Year'])['epu_score'].agg(['mean']).reset_index()

epu.rename(columns={"mean": "epu_mean"},inplace=True)

epu['epu_mean_diff']=epu.groupby('country')['epu_mean'].pct_change()
epu.dropna(inplace=True)

#get the highet % change (follow Cherawong 2020) you would like for each measure
def high_precentile(df,col):
        df['high_'+str(col)+'_10th_precntile']=df.groupby('country')[col].transform(lambda x: x.quantile(.9))
        df['high_'+str(col)+'_10th_indicator'] = np.where(df[col] > df['high_'+str(col)+'_10th_precntile'], 1, 0)
        df.drop(columns=['high_'+str(col)+'_10th_precntile'],inplace=True)
        return df
epu=high_precentile(epu,'epu_mean_diff')

# Generate high_epu_mean_diff_10th_indicator in prior Future years
epu['EPU_Sock_F_1'] = epu.groupby('country')['high_epu_mean_diff_10th_indicator'].shift(1)
epu['EPU_Sock_F_2'] = epu.groupby('country')['high_epu_mean_diff_10th_indicator'].shift(2)
# Generate high_epu_mean_diff_10th_indicator in Prior two years
epu['EPU_Sock_P_1'] = epu.groupby('country')['high_epu_mean_diff_10th_indicator'].shift(-1)
epu['EPU_Sock_P_2'] = epu.groupby('country')['high_epu_mean_diff_10th_indicator'].shift(-2)

#keep only periods from 2003
epu=epu[epu['Year']>=2003]

#merge with trade data
trade_merged = pd.merge(trade,epu,how='left',on=['Year','country']).dropna(subset=['epu_mean_diff']).reset_index(drop=True)

#merge us epu
epu_us = epu[epu['country']=='US']
epu_us.rename(columns={'high_epu_mean_diff_10th_indicator':'US_Shock'},inplace=True)
trade_merged = pd.merge(trade_merged,epu_us[["Year","US_Shock"]],how='left',on=['Year'])

trade_merged.to_stata('trade_merged.dta',write_index=False,version=118)

#plots

import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

trade_merged['Years_Relative_To_Shock'] = trade_merged.apply(
    lambda row: -1 if row['EPU_Sock_P_1'] == 1 else (
            0 if row['high_epu_mean_diff_10th_indicator'] == 1 else (
                1 if row['EPU_Sock_F_1'] == 1 else None)),
    axis=1
)

# Drop rows with None in Years_Relative_To_Shock if any
trade_merged = trade_merged.dropna(subset=['Years_Relative_To_Shock'])

# Calculate the average change in imports for each year relative to the shock for both variables
avg_import_change = trade_merged.groupby('Years_Relative_To_Shock').agg(
    {'CountryTradeRank_change': 'mean', 'Import_change': 'mean'}).reset_index()

# Plotting the data
fig, ax1 = plt.subplots(figsize=(12, 6))

def to_percent(y, position):
    return f'{100 * y:.0f}%'

# Create a second y-axis to plot Import_change
color = 'black'
ax1.set_ylabel('Average Change in U.S. Imports', color=color)
ax1.set_xlabel('Year to EPU Shock', color=color)
ax1.set_title('Figure 1: Average Change in U.S. Imports around EPU Spikes in Supplier Countries')
#ax1.set_xlabel( 'Year of EPU Spike in Supplier Country', color=color)
ax1.plot(avg_import_change['Years_Relative_To_Shock'], avg_import_change['Import_change'], marker='s', linestyle='--', color=color)
ax1.tick_params(axis='y', labelcolor=color)
ax1.axvline(x=0, color='r', linestyle='--')

plt.xlim(-1, 1)  # Set x-axis limits to start from 0 and end at 90

# Format y-axis as percentage
ax1.yaxis.set_major_formatter(FuncFormatter(to_percent))

# Title and grid
#plt.title('Average Change in Imports Relative to EPU Shock')
fig.tight_layout()
ax1.yaxis.grid(True)
plt.xticks([-1, 0, 1], ['t-1',"t", 't+1'])
fig.legend(loc="upper right", bbox_to_anchor=(0.9,0.9))

plt.show()