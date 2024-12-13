import subprocess

def psqldocker_up():

        # Command to run the docker-compose.yml file to start a PSQL instance
        run_psql = ['docker-compose', 'up', '-d']
        
        # Run the run_psql command
        try:
            subprocess.run(run_psql, check=True)
            print('PSQL instance successfully launched')
        
        except subprocess.CalledProcessError as e:
            print(f"Error starting the PSQL instance: {e}")

def psqldocker_down():

        # Command to stop the running PSQL instance
        run_psql = ['docker-compose', 'down', '-v']
        
        # Run the run_psql command
        try:
            subprocess.run(run_psql, check=True)
            print('PSQL instance successfully stopped')
        
        except subprocess.CalledProcessError as e:
            print(f"Error stopping the PSQL instance: {e}")
            