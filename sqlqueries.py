# Here are the list of queries to be performed when connected to the PSQL instance

def sql_query_creating_tables():
    
    command = '''BEGIN;


        CREATE TABLE IF NOT EXISTS public."dateDimTable"
        (
            date_id integer NOT NULL,
            saledate date NOT NULL,
            saledate_year integer NOT NULL,
            saledate_month integer NOT NULL,
            saledate_monthname character varying(20) NOT NULL,
            saledate_day integer NOT NULL,
            saledate_weekday integer NOT NULL,
            saledate_weekdayname character varying(20) NOT NULL,
            quarter integer NOT NULL,
            quartername character varying(2) NOT NULL,
            PRIMARY KEY (date_id)
        );

        CREATE TABLE IF NOT EXISTS public."vehicleDimTable"
        (
            vehicle_id integer NOT NULL,
            year integer NOT NULL,
            make character varying(30) NOT NULL,
            model character varying(30) NOT NULL,
            "trim" character varying(100) NOT NULL,
            body character varying(30) NOT NULL,
            transmission character varying(30) NOT NULL,
            color character varying(30) NOT NULL,
            interior character varying(30) NOT NULL,
            PRIMARY KEY (vehicle_id)
        );

        CREATE TABLE IF NOT EXISTS public."sellerDimTable"
        (
            seller_id integer NOT NULL,
            seller character varying(50) NOT NULL,
            PRIMARY KEY (seller_id)
        );

        CREATE TABLE IF NOT EXISTS public."stateDimTable"
        (
            state_id integer NOT NULL,
            state character varying(5) NOT NULL,
            PRIMARY KEY (state_id)
        );

        CREATE TABLE IF NOT EXISTS public."salesFactTable"
        (
            sale_id integer NOT NULL,
            vin character varying(50) NOT NULL,
            vehicle_id integer NOT NULL,
            state_id integer NOT NULL,
            seller_id integer NOT NULL,
            mmr numeric(9, 2) NOT NULL,
            sellingprice numeric(9, 2) NOT NULL,
            odometer integer NOT NULL,
            condition integer NOT NULL,
            date_id integer NOT NULL,
            PRIMARY KEY (sale_id)
        );

        ALTER TABLE IF EXISTS public."salesFactTable"
            ADD FOREIGN KEY (vehicle_id)
            REFERENCES public."vehicleDimTable" (vehicle_id) MATCH SIMPLE
            ON UPDATE NO ACTION
            ON DELETE NO ACTION
            NOT VALID;


        ALTER TABLE IF EXISTS public."salesFactTable"
            ADD FOREIGN KEY (state_id)
            REFERENCES public."stateDimTable" (state_id) MATCH SIMPLE
            ON UPDATE NO ACTION
            ON DELETE NO ACTION
            NOT VALID;


        ALTER TABLE IF EXISTS public."salesFactTable"
            ADD FOREIGN KEY (seller_id)
            REFERENCES public."sellerDimTable" (seller_id) MATCH SIMPLE
            ON UPDATE NO ACTION
            ON DELETE NO ACTION
            NOT VALID;


        ALTER TABLE IF EXISTS public."salesFactTable"
            ADD FOREIGN KEY (date_id)
            REFERENCES public."dateDimTable" (date_id) MATCH SIMPLE
            ON UPDATE NO ACTION
            ON DELETE NO ACTION
            NOT VALID;

        END;'''
        
    return command

def query_1():
    
    query = 'SELECT * FROM public."dateDimTable" LIMIT 5;'           
    return query

def query_2():
    
    query = 'SELECT * FROM public."sellerDimTable" LIMIT 5;'           
    return query

def query_3():
    
    query = 'SELECT * FROM public."stateDimTable" LIMIT 5;'           
    return query

def query_4():
    
    query = 'SELECT * FROM public."vehicleDimTable" LIMIT 5;'           
    return query

def query_5():
    
    query = 'SELECT * FROM public."salesFactTable" LIMIT 5;'           
    return query

def query_6():
    
    query = '''SELECT v.make, COUNT(v.make) AS units_sold FROM "salesFactTable" AS s LEFT JOIN "dateDimTable" AS d 
            ON s.date_id = d.date_id LEFT JOIN "sellerDimTable" AS sel ON sel.seller_id = s.seller_id LEFT JOIN "stateDimTable" AS st 
            ON st.state_id = s.state_id LEFT JOIN "vehicleDimTable" AS v ON v.vehicle_id = s.vehicle_id GROUP BY v.make ORDER BY units_sold DESC LIMIT 10;'''         
    return query

def query_7():
    
    query = '''SELECT d.saledate_year, d.quartername, SUM(s.mmr) AS total_sales FROM "salesFactTable" AS s LEFT JOIN "dateDimTable" AS d 
            ON s.date_id = d.date_id LEFT JOIN "sellerDimTable" AS sel ON sel.seller_id = s.seller_id LEFT JOIN "stateDimTable" AS st 
            ON st.state_id = s.state_id LEFT JOIN "vehicleDimTable" AS v ON v.vehicle_id = s.vehicle_id GROUP BY d.saledate_year, d.quartername
            ORDER BY total_sales DESC;'''         
    return query

