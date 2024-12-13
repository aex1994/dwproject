This project:

1. Installs dependencies such as pandas, psql-client CLI, pyscopg2, kaggle api, and python-dotenv
2. Imports data from Kaggle
3. Transforms the data from Kaggle to dimension tables and fact table and save them to CSV format
4. Sets up a PSQL instance using Docker Compose
5. Creates dimension tables and fact table in the PSQL instance
6. Inserts the values from the CSV files to the appropriate tables in the PSQL instance
7. Queries the PSQL instance to verify the accuracy of the data warehouse

Manual Setup Needed:

1. Manually install Docker Desktop (https://www.docker.com/products/docker-desktop/) in your machine and setup WSL2 integration (https://docs.docker.com/desktop/features/wsl/). Make sure Docker Desktop is running for the duration of the script.
2. All contents of this folder should be in a folder in your home directory called "dwproject". The working project directory is "~/dwproject".
3. Rename the temp.env file to .env
4. Create a Kaggle account (https://www.kaggle.com/) and generate your API token. The downloaded API token, kaggle.json, should be copied inside this directory "~/.config/kaggle"
5. Run this bash command inside the project folder "~/dwproject" sudo chmod 777 mainscript.sh
6. Run mainscript.sh using the command ./mainscript.sh
7. Additionally, you can run ./rmcsvfiles.sh if you want to delete all generated csv files to save space
8. This line is added to test commit