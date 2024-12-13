import subprocess

def install_pandas():
    
    # Command to install pandas library
    pip_command = ['pip', 'install', 'pandas']
    
    # Run the pip_command
    try:
        subprocess.run(pip_command, check=True)
        print('Pandas succesfully installed')
    except subprocess.CalledProcessError as e:
        print(f"Error installing Pandas: {e}")
        
def install_kaggle_api():
    
    # Command to install the Kaggle API
    pip_command = ['pip', 'install', 'kaggle']
    
    # Run the pip_command
    try:
        subprocess.run(pip_command, check=True)
        print('Kaggle API succesfully installed')
    except subprocess.CalledProcessError as e:
        print(f"Error installing Kaggle API: {e}")
        
def install_pyscopg2():
    
    # Command to install the pyscopg2 connector
    pip_command = ['pip', 'install', 'psycopg2-binary']
    
    # Run the pip_command
    try:
        subprocess.run(pip_command, check=True)
        print('Pyscopg2-binary succesfully installed')
    except subprocess.CalledProcessError as e:
        print(f"Error installing psycopg2-binary: {e}")
        
def install_psql_cli():
    
    # Command to install the psql cli version 16
    apt_command = ['sudo', 'apt', 'install', 'postgresql-client-16']
    
    # Run the apt_command
    try:
        subprocess.run(apt_command, check=True)
        print('Psql CLI version 16 successfully installed')
    except subprocess.CalledProcessError as e:
        print(f"Error Psql CLI: {e}")
        
def install_pydotenv():
    
    # Command to install the pyscopg2 connector
    pip_command = ['pip', 'install', 'python-dotenv']
    
    # Run the pip_command
    try:
        subprocess.run(pip_command, check=True)
        print('Python-dotenv succesfully installed')
    except subprocess.CalledProcessError as e:
        print(f"Error installing python-dotenv: {e}")

def main():
    
    print('Installing dependencies...')
    install_kaggle_api()
    install_pandas()
    install_pydotenv()
    install_pyscopg2()
    install_psql_cli()
    
if __name__ == '__main__':
    main()
    