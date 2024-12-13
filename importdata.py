import subprocess
import os

def importdata():
    
    # Expand the '~' to the full home directory path
    project_dir = os.path.expanduser('~/dwproject')
    
    # Command to download the dataset using Kaggle CLI
    download_command = [
        'kaggle', 'datasets', 'download', 'syedanwarafridi/vehicle-sales-data',
        '-p', project_dir, '--unzip'
    ]

    # Command to rename the file after downloading
    rename_command = [
        'mv', os.path.join(project_dir, 'car_prices.csv'), os.path.join(project_dir, 'Vehicle_sales_data.csv')
    ]

    # Run the download command
    try:
        subprocess.run(download_command, check=True)
        print("Dataset downloaded and unzipped successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error downloading the dataset: {e}")

    # Run the rename command
    try:
        subprocess.run(rename_command, check=True)
        print("File renamed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error renaming the file: {e}")
