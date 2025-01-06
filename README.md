# Data Warehousing Project for Car Sales Kaggle Dataset

## Overview:

1. Installs dependencies such as pandas, psql-client CLI, pyscopg2, kaggle api, and python-dotenv
2. Imports data from Kaggle
3. Transforms the data from Kaggle to dimension tables and fact table and saves them to CSV format. Duplicate and missing values were handled. 
4. Sets up a PostgreSQL instance using Docker Compose
5. Creates dimension tables and fact table in the PostgreSQL instance
6. Inserts the data from the CSV files to the appropriate tables in the PostgreSQL instance
7. Queries the PostgreSQL instance to verify the accuracy of the data warehouse.

## Manual Setup Needed:

1. The project was developed in a WSL2 enivronment in Windows 11 using Ubuntu 24.04 as distro and VSCode as IDE. If WSL2 is not yet enabled in your machine, follow these instrcutions [https://learn.microsoft.com/en-us/windows/wsl/install](https://learn.microsoft.com/en-us/windows/wsl/install).
2. Manually install Docker Desktop [https://www.docker.com/products/docker-desktop/](https://www.docker.com/products/docker-desktop/) in your machine and setup WSL2 integration [https://docs.docker.com/desktop/features/wsl/](https://docs.docker.com/desktop/features/wsl/). Make sure Docker Desktop is running for the duration of the script.
3. Make sure you have Python 3 installed in your machine along with pip. You can follow these instrcutions [https://learn.microsoft.com/en-us/windows/python/web-frameworks](https://learn.microsoft.com/en-us/windows/python/web-frameworks).
4. All contents of this folder should be in a folder in your home directory called **dwproject**. The working project directory is **~/dwproject**. You can just clone this repo in your home folder using ```git clone https://github.com/aex1994/dwproject```.
5. Rename the **env.txt** file to **.env**
6. Create a Kaggle account [https://www.kaggle.com/](https://www.kaggle.com/) and generate your API token. The downloaded API token, kaggle.json, should be copied inside this directory **~/.config/kaggle**
7. Run this bash command inside the project folder **~/dwproject**
```bash 
sudo chmod 777 mainscript.sh
```
8. Run mainscript.sh using the command 
```bash
./mainscript.sh
```
9. Additionally, you can run
```bash
./rmcsvfiles.sh
``` 
if you want to delete all generated csv files to save storage space. Just make sure to perform 
```bash
sudo chmod 777 rmcsvfiles.sh
```

## Pipeline:

### mydependencies.py

This python code installs the following:

1. kaggle API -> for extracting the car_sales dataset in Kaggle
2. pandas -> for transforming the dataset from kaggle to CSV files structured as fact table and dimension tables
3. psycopg2-bin -> for connecting to the PostgreSQL instance and creating the necessary tables suited for the CSV files generated from the transformation
4. postgresql-client-16 -> for connecting to the PostgreSQL instance and running sample queries
5. python-dotenv -> for python to access the .env file where credentials are stored and used in connecting to the PostgreSQL instance

![screenshot_mydependcies.py](img/mydependencies.png)

### importdata.py

This python code extracts the car sales dataset from Kaggle and renames it to **Vehicle_sales_data.csv**

![screenshot_importdata.py](img/importdata.png)

### transform.py

This python code uses pandas to make the necessary transformations on the dataset and load them into CSV files. The transformation includes:

1. Dropping of duplicate rows by checking duplicate **vin** values because **vin** is supposed to be unique for each row:
```python
df.drop_duplicates(subset='vin', inplace=True)
```
2. Handling missing values using the following functions:

    a. fillna_with_mode -> imputes missing values by getting the mode of for the target column based on a reference column. For example, when imputing missing values for the column **make**, instead of getting the mode of the entire **make** column, it is grouped by the column **seller**, then the mode for each group is used to impute the missing values for the **make** column. This is smart way of mitigating the errors and uncertainty that comes with missing value imputation. In cases of two or more modes, the first mode will be used and if there are no modes, the value is **np.nan** to denote an empty value.

    ```python
    def fillna_with_mode(target_col, reference_col, dataframe):
    
    most_freq = dataframe.groupby(reference_col)[target_col] \
        .agg(lambda x: x.mode()[0] if not x.mode().empty else np.nan)
    most_freq = most_freq.reset_index(name = 'most_freq')
    
    merged = pd.merge(dataframe, most_freq, on=reference_col, how='left')
    dataframe[target_col] = dataframe[target_col].fillna(merged['most_freq'])
    return dataframe
    ```

    b. fillna_with_mean -> imputes missing values for the columns with numerical values such as the **mmr** and **sellingprice** columns. No further strategies were employed here unlike the function above.

    ```python
    def fillna_with_mean(target_col, dataframe):
    
    mean_value = int(dataframe[target_col].mean())
    dataframe[target_col] = dataframe[target_col].fillna(mean_value)
    return dataframe
    ```
3. Transforming the date column to a more structured format for data warehousing.

    a. Extracting the date part from the exisiting **saledate** column using string splicing
    ```python
    df['saledate_new'] = df['saledate'].str[4:15]
    ```
    b. Extracting the weekday name from the existing **saledate** column using string splicing
    ```python
    df['saledate_weekdayname'] = df['saledate'].str[0:3]
    ```
    c. Mapping the the weekday name to a weekday numerical value using a function **generate_weekday**. Ex. Mon = 1, Tue = 2.
    ```python
    def generate_weekday(dataframe):
    weekday_map = {'Mon':1, 'Tue':2, 'Wed':3, 'Thu':4, 'Fri':5, 'Sat':6, 'Sun':7}
    dataframe['saledate_weekday'] = dataframe['saledate_weekdayname'].map(weekday_map)
    return dataframe
    ```
    ```python
    df = generate_weekday(df)
    ```
    d. Casting the **saledate_new** column to pandas datetime format for easier extraction of the year, month, month name quarter, quarter name, day
    ```python
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
    ```
4. After all the imputations are done, all other rows that still have a missing value will be dropped from the dataset.

5. Final casting of data types to all the columns of the dataframe:

```python
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
```
6. Summarizes the transformation showing how many rows were dropped and the data type of each column.

![transform1](img/transform1.png)

7. Final check if there are missing values in the dataset and a sneak peek ot the first 5 rows of the dataframe

![transform2](img/transform2.png)

8. Creating multiple dataframes by splicing the original dataframe. This is done so that we can load the multiple dataframes into CSV files which are suited for fact table and dimension tables.

```python
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
```
![transform3](img/transform3.png)



