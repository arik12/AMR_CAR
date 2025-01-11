import pandas as pd
import re
import numpy as np
import math
from datetime import datetime, timedelta,date
import os
import time
import glob
from scipy.stats import mstats
from pandas.tseries.offsets import MonthEnd

pd.set_option('display.expand_frame_repr', False)
# pd.set_option('display.max_colwidth', -1)
# Reduce decimal points to 2
pd.options.display.float_format = '{:,.3f}'.format

#set my desired working directory
os.chdir("")

#load EPU data: Note to make sure the Baker et al. (2016) data is in  Year Month Country columns format (e.g., "year", "month", "China", "India" etc.).
epu_raw=pd.read_excel('EPU.xlsx')

#get the end of the month in a good format
epu_raw['date']=(pd.to_datetime(epu_raw[['Year', 'Month']].assign(day='1'), format="%Y%m")+ MonthEnd(1)).dt.strftime('%Y%m%d')

#trransform to a panel
epu=epu_raw.melt(id_vars=['date','Year','Month'],
        var_name="country",
        value_name="epu_score").reset_index(drop=True)
#drop na
epu=epu.dropna(subset=['epu_score']).reset_index(drop=True)

epu['date_quarter'] = pd.PeriodIndex(pd.to_datetime(epu.date), freq='Q')  # generate quarter

#genereate mean EPU score per quarter
epu=epu.groupby(['country','date_quarter'])['epu_score'].agg(['max','min','mean']).reset_index()

epu.rename(columns={"max": "epu_max", "min": "epu_min", "mean": "epu_mean"},inplace=True)


epu['epu_mean_diff']=epu.groupby('country')['epu_mean'].pct_change()
epu.dropna(inplace=True)

#get the highet % change (follow Cherawong 2020) you would like for each measure
def high_precentile(df,col):
        df['high_'+str(col)+'_10th_precntile']=df.groupby('country')[col].transform(lambda x: x.quantile(.9))
        df['high_'+str(col)+'_10th_indicator'] = np.where(df[col] > df['high_'+str(col)+'_10th_precntile'], 1, 0)
        df.drop(columns=['high_'+str(col)+'_10th_precntile'],inplace=True)
        return df

epu=high_precentile(epu,'epu_mean_diff')

epu.to_csv('epu_time_series.csv')