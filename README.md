# This project:

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