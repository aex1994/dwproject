import pandas as pd
import numpy as np

#function to return a df where missing values are imputed using the mode based on a relevant column
def fillna_with_mode(target_col, reference_col, dataframe):
    
    most_freq = dataframe.groupby(reference_col)[target_col] \
        .agg(lambda x: x.mode()[0] if not x.mode().empty else np.nan)
    most_freq = most_freq.reset_index(name = 'most_freq')
    
    merged = pd.merge(dataframe, most_freq, on=reference_col, how='left')
    dataframe[target_col] = dataframe[target_col].fillna(merged['most_freq'])
    return dataframe

#function to impute missing values in the target column with the mean value of the target column
def fillna_with_mean(target_col, dataframe):
    
    mean_value = int(dataframe[target_col].mean())
    dataframe[target_col] = dataframe[target_col].fillna(mean_value)
    return dataframe

def generate_weekday(dataframe):
    weekday_map = {'Mon':1, 'Tue':2, 'Wed':3, 'Thu':4, 'Fri':5, 'Sat':6, 'Sun':7}
    dataframe['saledate_weekday'] = dataframe['saledate_weekdayname'].map(weekday_map)
    return dataframe

#read the csv file into a pandas dataframe
df = pd.read_csv(r'~/dwproject/Vehicle_sales_data.csv')

#get initial length of df
start_length = len(df)

#drop rows that corresponds to duplicates in the vin column
df.drop_duplicates(subset='vin', inplace=True)

#fill missing make values using mode of make grouped by seller
df = fillna_with_mode('make', 'seller', df)
df.dropna(subset='make', inplace=True)

#fill missing model values using mode of model grouped by make
df = fillna_with_mode('model', 'make', df)
df.dropna(subset='model', inplace=True)

#fill missing trim values using mode of trim grouped by model
df = fillna_with_mode('trim', 'model', df)
df.dropna(subset='trim', inplace=True)

#fill missing body values using mode of body grouped by model
df = fillna_with_mode('body', 'model', df)
df.dropna(subset='body', inplace=True)

#fill missing transmission values using mode of transmission grouped by model
df = fillna_with_mode('transmission', 'model', df)
df.dropna(subset='transmission', inplace=True)

#drop state values where length of input is more than 2
df = df[df['state'].str.len() == 2]

#fill missing values in the condition column with the mean value of the condition column
df = fillna_with_mean('condition', df)

#drop rows with missing odometer values
df.dropna(subset='odometer', inplace=True)

#drop rows with missing color values and incorrect color such as '-' and 2-letter word or less values
df.dropna(subset='color', inplace=True)
df = df[df['color'].str.len() >= 3]

#drop rows with missing mmr values
df.dropna(subset='mmr', inplace=True)

#parse the saledate column to get just the date part and store in a new column
df['saledate_new'] = df['saledate'].str[4:15]
#parse the saledate column to get just the weekdayname and store in a new column
df['saledate_weekdayname'] = df['saledate'].str[0:3]
#add anew column saledate_weekday corresponding to weekdayname, Mon = 1, Tue = 2, etc.
df = generate_weekday(df)
#format saledate_new to datetime
df['saledate_new'] = pd.to_datetime(df['saledate_new'])
#drop the saledate column then rename saledate_new to saledate
df.drop('saledate', axis=1, inplace=True)
df.rename(columns={'saledate_new': 'saledate'}, inplace=True)
#add a column for year, month and day
df['saledate_year'] = df['saledate'].dt.year
df['saledate_month'] = df['saledate'].dt.month
df['saledate_monthname'] = df['saledate'].dt.month_name()
df['saledate_day'] = df['saledate'].dt.day
#add a quarter and a quartername column
df['quarter'] = df['saledate'].dt.quarter
df['quartername'] = 'Q' + df['quarter'].astype(str)

#cast correct data types for each columns
df['year'] = df['year'].astype(int)
df['make'] = df['make'].astype('string')
df['model'] = df['model'].astype('string')
df['trim'] = df['trim'].astype('string')
df['body'] = df['body'].astype('string')
df['transmission'] = df['transmission'].astype('string')
df['vin'] = df['vin'].astype('string')
df['state'] = df['state'].astype('string')
df['condition'] = df['condition'].astype(int)
df['odometer'] = df['odometer'].astype(int)
df['color'] = df['color'].astype('string')
df['interior'] = df['interior'].astype('string')
df['seller'] = df['seller'].astype('string')
df['mmr'] = df['mmr'].astype(float)
df['sellingprice'] = df['sellingprice'].astype(float)
df['saledate'] = pd.to_datetime(df['saledate'])
df['saledate_year'] = df['saledate_year'].astype(int)
df['saledate_month'] = df['saledate_month'].astype(int)
df['saledate_monthname'] = df['saledate_monthname'].astype('string')
df['saledate_day'] = df['saledate_day'].astype(int)
df['saledate_weekdayname'] = df['saledate_weekdayname'].astype('string')
df['saledate_weekday'] = df['saledate_weekday'].astype(int)
df['quarter'] = df['quarter'].astype(int)
df['quartername'] = df['quartername'].astype('string')

end_length = len(df)
difference = start_length - end_length

print('Dataframe Summary')
print(f'Before Transformation: {start_length}')
print(f'After Transformation: {end_length}')
print(f'Dropped Rows: {difference} ({(difference/start_length)*100:.2f}%)')
print('Data types for each column:')
print(df.dtypes)
print('Are there null values in each column?')
print(df.isnull().any())
print('Printing top 5 rows')
print(df.head())

#make different dataframes that will correspond to the diffrent tables in the star schema
#dateDimTable

print('Creating CSV files for the dimension tables and fact table')

dateDimTable = df[['saledate', 'saledate_year', 'saledate_month', 'saledate_monthname','saledate_day',
                   'saledate_weekday', 'saledate_weekdayname', 'quarter', 'quartername', 
                   ]].drop_duplicates()
dateDimTable = dateDimTable.copy()
dateDimTable['date_id'] = dateDimTable.reset_index().index+1
dateDimTable.to_csv('~/dwproject/dateDimTable.csv', index=False)
print('dateDimTable.csv created')

#sellerDimTable
sellerDimTable = df[['seller']].drop_duplicates()
sellerDimTable = sellerDimTable.copy()
sellerDimTable['seller_id'] = sellerDimTable.reset_index().index+1
sellerDimTable.to_csv('~/dwproject/sellerDimTable.csv', index=False)
print('sellerDimTable.csv created')

#stateDimTable
stateDimTable = df[['state']].drop_duplicates()
stateDimTable = stateDimTable.copy()
stateDimTable['state_id'] = stateDimTable.reset_index().index+1
stateDimTable.to_csv('~/dwproject/stateDimTable.csv', index=False)
print('stateDimTable.csv created')

#vehicleDimTable
vehicleDimTable = df[['year', 'make', 'model', 'trim', 'body', 'transmission', 'color', 'interior']].drop_duplicates()
vehicleDimTable = vehicleDimTable.copy()
vehicleDimTable['vehicle_id'] = vehicleDimTable.reset_index().index+1
vehicleDimTable.to_csv('~/dwproject/vehicleDimTable.csv', index=False)
print('vehicleDimTable.csv created')

#salesFactTable
salesFactTable = df.copy()
salesFactTable = salesFactTable.merge(dateDimTable[['date_id', 'saledate']], on='saledate', how='left')
salesFactTable = salesFactTable.merge(sellerDimTable[['seller_id', 'seller']], on='seller', how='left')
salesFactTable = salesFactTable.merge(stateDimTable[['state_id', 'state']], on='state', how='left')
salesFactTable = salesFactTable.merge(vehicleDimTable[['vehicle_id', 'year', 'make', 'model', 'trim', 'body', 
                                                       'transmission', 'color', 'interior']], 
                                      on=['year', 'make', 'model', 'trim', 'body', 'transmission', 'color', 'interior'], 
                                      how='left')

#Drop Redundant Columns (i.e., columns that are now part of the dimension tables)
salesFactTable = salesFactTable.drop(columns=['year', 'make', 'model', 'trim', 'body', 'transmission',
                                              'color', 'interior', 'saledate', 'seller', 'state',
                                              'saledate_weekdayname','saledate_weekday', 'saledate_year',
                                              'saledate_month', 'saledate_monthname', 'saledate_day',
                                              'quarter', 'quartername'])
salesFactTable['sale_id'] = salesFactTable.reset_index().index+1
salesFactTable.to_csv('~/dwproject/salesFactTable.csv', index=False)
print('salesFactTable.csv created')