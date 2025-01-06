# Data Warehousing Project Overview:

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
7. Run this bash command inside the project folder **~/dwproject** ```sudo chmod 777 mainscript.sh```
8. Run mainscript.sh using the command ```./mainscript.sh```
9. Additionally, you can run ```./rmcsvfiles.sh``` if you want to delete all generated csv files to save storage space. Just make sure to perform ```sudo chmod 777 rmcsvfiles.sh```

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

1. Dropping of duplicate rows by checking duplicate vin values because vin is supposed to be unique for each row. ```df.drop_duplicates(subset='vin', inplace=True)```
2. Handling missing values using the following functions:

    a. fillna_with_mode -> imputes missing values by getting the mode of for the target column based on a reference column. For example, when imputing missing values for the column **make**, instead of getting the mode of the entire **make** column, it is grouped by the column **seller**, then the mode for each group is used to impute the missing values for the **make** column. This is smart way of mitigating the errors and uncertainty that comes with missing value imputation.

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
3. Transforming the date column to a more structured format

