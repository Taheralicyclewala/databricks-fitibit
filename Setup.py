# Databricks notebook source
# MAGIC %md
# MAGIC ### Setup Script for Creating Required Tables

# COMMAND ----------

class Config:
    def __init__(self):
        self.base_dir_data = spark.sql("DESCRIBE EXTERNAL LOCATION `data_zone`").select("url").collect()[0][0]
        self.base_dir_checkpoint = spark.sql("DESCRIBE EXTERNAL LOCATION `checkpoint`").select("url").collect()[0][0]
        self.bronze_db_name = 'fitbit_bronze'
        self.silver_db_name = 'fitbit_silver'
        self.gold_db_name = 'fitbit_gold'

class SetupHelper():   
    def __init__(self, env):
        conf = Config()
        self.landing_zone = conf.base_dir_data + "/raw"
        self.checkpoint_base = conf.base_dir_checkpoint + "/checkpoints"        
        self.catalog = f"fitbit-{env}"
        self.bz_db_name = conf.bronze_db_name
        self.silver_db_name = conf.silver_db_name
        self.gold_db_name = conf.gold_db_name
        self.initialized = False
        
    def create_db(self, db_name):
        spark.catalog.clearCache()
        print(f"Creating the database {self.catalog}.{db_name}...", end='')
        spark.sql(f"CREATE DATABASE IF NOT EXISTS `{self.catalog}`.{db_name}")
        spark.sql(f"USE `{self.catalog}`.{db_name}")
        self.initialized = True
        print("Done")
        
    def create_registered_users_bz(self, db_name):
        if(self.initialized):
            print(f"Creating registered_users_bz table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS `{self.catalog}`.{db_name}.registered_users_bz(
                    user_id long,
                    device_id long, 
                    mac_address string, 
                    registration_timestamp double,
                    load_time timestamp,
                    source_file string                    
                    )
                  """) 
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
    
    def create_gym_logins_bz(self, db_name):
        if(self.initialized):
            print(f"Creating gym_logins_bz table...", end='')
            spark.sql(f"""CREATE OR REPLACE TABLE `{self.catalog}`.{db_name}.gym_logins_bz(
                    mac_address string,
                    gym bigint,
                    login double,                      
                    logout double,                    
                    load_time timestamp,
                    source_file string
                    )
                  """) 
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
            
    def create_kafka_multiplex_bz(self, db_name):
        if(self.initialized):
            print(f"Creating kafka_multiplex_bz table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS `{self.catalog}`.{db_name}.kafka_multiplex_bz(
                  key string, 
                  value string, 
                  topic string, 
                  partition bigint, 
                  offset bigint, 
                  timestamp bigint,                  
                  date date, 
                  week_part string,                  
                  load_time timestamp,
                  source_file string)
                  PARTITIONED BY (topic, week_part)
                  """) 
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")       
    
            
    def create_users(self,db_name):
        if(self.initialized):
            print(f"Creating users table...", end='')
            spark.sql(f"""CREATE OR REPLACE TABLE `{self.catalog}`.{db_name}.users(
                    user_id bigint, 
                    device_id bigint, 
                    mac_address string,
                    registration_timestamp timestamp
                    )
                  """)  
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")            
    
    def create_gym_logs(self, db_name):
        if(self.initialized):
            print(f"Creating gym_logs table...", end='')
            spark.sql(f"""CREATE OR REPLACE TABLE `{self.catalog}`.{db_name}.gym_logs(
                    mac_address string,
                    gym bigint,
                    login timestamp,                      
                    logout timestamp
                    )
                  """) 
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
    def create_user_profile(self, db_name):
        if(self.initialized):
            print(f"Creating user_profile table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS `{self.catalog}`.{db_name}.user_profile(
                    user_id bigint, 
                    dob DATE, 
                    sex STRING, 
                    gender STRING, 
                    first_name STRING, 
                    last_name STRING, 
                    street_address STRING, 
                    city STRING, 
                    state STRING, 
                    zip INT, 
                    updated TIMESTAMP)
                  """)  
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

    def create_heart_rate(self, db_name):
        if(self.initialized):
            print(f"Creating heart_rate table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS `{self.catalog}`.{db_name}.heart_rate(
                    device_id LONG, 
                    time TIMESTAMP, 
                    heartrate DOUBLE, 
                    valid BOOLEAN)
                  """)
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

            
    def create_user_bins(self, db_name):
        if(self.initialized):
            print(f"Creating user_bins table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS `{self.catalog}`.{db_name}.user_bins(
                    user_id BIGINT, 
                    age STRING, 
                    gender STRING, 
                    city STRING, 
                    state STRING)
                  """)  
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
            
    def create_workouts(self, db_name):
        if(self.initialized):
            print(f"Creating workouts table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS `{self.catalog}`.{db_name}.workouts(
                    user_id INT, 
                    workout_id INT, 
                    time TIMESTAMP, 
                    action STRING, 
                    session_id INT)
                  """)  
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
            
    def create_completed_workouts(self, db_name):
        if(self.initialized):
            print(f"Creating completed_workouts table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS `{self.catalog}`.{db_name}.completed_workouts(
                    user_id INT, 
                    workout_id INT, 
                    session_id INT, 
                    start_time TIMESTAMP, 
                    end_time TIMESTAMP)
                  """)  
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
            
    def create_workout_bpm(self, db_name):
        if(self.initialized):
            print(f"Creating workout_bpm table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS `{self.catalog}`.{db_name}.workout_bpm(
                    user_id INT, 
                    workout_id INT, 
                    session_id INT,
                    start_time TIMESTAMP, 
                    end_time TIMESTAMP,
                    time TIMESTAMP, 
                    heartrate DOUBLE)
                  """)  
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
            
    def create_date_lookup(self, db_name):
        if(self.initialized):
            print(f"Creating date_lookup table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS `{self.catalog}`.{db_name}.date_lookup(
                    date date, 
                    week int, 
                    year int, 
                    month int, 
                    dayofweek int, 
                    dayofmonth int, 
                    dayofyear int, 
                    week_part string)
                  """)  
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
    def create_workout_bpm_summary(self,db_name):
        if(self.initialized):
            print(f"Creating workout_bpm_summary table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS `{self.catalog}`.{db_name}.workout_bpm_summary(
                    workout_id INT, 
                    session_id INT, 
                    user_id BIGINT, 
                    age STRING, 
                    gender STRING, 
                    city STRING, 
                    state STRING, 
                    min_bpm DOUBLE, 
                    avg_bpm DOUBLE, 
                    max_bpm DOUBLE, 
                    num_recordings BIGINT)
                  """)
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
    def create_gym_summary(self,db_name):
        if(self.initialized):
            print(f"Creating gym_summar gold view...", end='')
            spark.sql(f"""CREATE OR REPLACE VIEW `{self.catalog}`.`{db_name}`.gym_summary AS
                            SELECT to_date(login::timestamp) date,
                            gym, l.mac_address, workout_id, session_id, 
                            round((logout::long - login::long)/60,2) minutes_in_gym,
                            round((end_time::long - start_time::long)/60,2) minutes_exercising
                            FROM `{self.catalog}`.`{self.silver_db_name}`.gym_logs l 
                            JOIN (
                            SELECT mac_address, workout_id, session_id, start_time, end_time
                            FROM `{self.catalog}`.`{self.silver_db_name}`.completed_workouts w INNER JOIN `{self.catalog}`.`{self.silver_db_name}`.users u ON w.user_id = u.user_id) w
                            ON l.mac_address = w.mac_address 
                            AND w. start_time BETWEEN l.login AND l.logout
                            order by date, gym, l.mac_address, session_id
                        """)
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
            
    def setup(self):
        import time
        start = int(time.time())
        print(f"\nStarting setup ...")
        self.create_db(self.bz_db_name)       
        self.create_registered_users_bz(self.bz_db_name)
        self.create_gym_logins_bz(self.bz_db_name) 
        self.create_kafka_multiplex_bz(self.bz_db_name)        
        self.create_db(self.silver_db_name)       
        self.create_users(self.silver_db_name)
        self.create_gym_logs(self.silver_db_name)
        self.create_user_profile(self.silver_db_name)
        self.create_heart_rate(self.silver_db_name)
        self.create_workouts(self.silver_db_name)
        self.create_completed_workouts(self.silver_db_name)
        self.create_workout_bpm(self.silver_db_name)
        self.create_user_bins(self.silver_db_name)
        self.create_date_lookup(self.silver_db_name)
        self.create_db(self.gold_db_name)       
        self.create_workout_bpm_summary(self.gold_db_name)  
        self.create_gym_summary(self.gold_db_name)
        print(f"Setup completed in {int(time.time()) - start} seconds")
        
    def assert_table(self, db_name, table_name):
        assert spark.sql(f"SHOW TABLES IN `{self.catalog}`.{db_name}") \
                   .filter(f"isTemporary == false and tableName == '{table_name}'") \
                   .count() == 1, f"The table {table_name} is missing"
        print(f"Found {table_name} table in {self.catalog}.{db_name}: Success")
        
    def validate(self):
        import time
        start = int(time.time())
        print(f"\nStarting setup validation ...")
        assert spark.sql(f"SHOW DATABASES IN `{self.catalog}`") \
                    .filter(f"databaseName == '{self.bz_db_name}'") \
                    .count() == 1, f"The database '{self.catalog}.{self.bz_db_name}' is missing"
        print(f"Found database {self.catalog}.{self.bz_db_name}: Success")
        assert spark.sql(f"SHOW DATABASES IN `{self.catalog}`") \
                    .filter(f"databaseName == '{self.silver_db_name}'") \
                    .count() == 1, f"The database '{self.catalog}.{self.silver_db_name}' is missing"
        print(f"Found database {self.catalog}.{self.silver_db_name}: Success")
        assert spark.sql(f"SHOW DATABASES IN `{self.catalog}`") \
                    .filter(f"databaseName == '{self.gold_db_name}'") \
                    .count() == 1, f"The database '{self.catalog}.{self.gold_db_name}' is missing"
        print(f"Found database {self.catalog}.{self.gold_db_name}: Success")
        self.assert_table(self.bz_db_name, "registered_users_bz")   
        self.assert_table(self.bz_db_name, "gym_logins_bz")        
        self.assert_table(self.bz_db_name, "kafka_multiplex_bz")
        self.assert_table(self.silver_db_name, "users")
        self.assert_table(self.silver_db_name, "gym_logs")
        self.assert_table(self.silver_db_name, "user_profile")
        self.assert_table(self.silver_db_name, "heart_rate")
        self.assert_table(self.silver_db_name, "workouts")
        self.assert_table(self.silver_db_name, "completed_workouts")
        self.assert_table(self.silver_db_name, "workout_bpm")
        self.assert_table(self.silver_db_name, "user_bins")
        self.assert_table(self.silver_db_name, "date_lookup")
        self.assert_table(self.gold_db_name, "workout_bpm_summary") 
        self.assert_table(self.gold_db_name, "gym_summary") 
        print(f"Setup validation completed in {int(time.time()) - start} seconds")
        
    def cleanup(self): 
        for db_name in (self.bz_db_name, self.silver_db_name, self.gold_db_name):
            if spark.sql(f"SHOW DATABASES IN `{self.catalog}`").filter(f"databaseName == '{db_name}'").count() == 1:
                print(f"Dropping the database {self.catalog}.{db_name}...", end='')
                spark.sql(f"""DROP DATABASE `{self.catalog}`.{db_name} CASCADE""")
                print("Done")
        print(f"Deleting {self.landing_zone}...", end='')
        dbutils.fs.rm(self.landing_zone, True)
        print("Done")
        print(f"Deleting {self.checkpoint_base}...", end='')
        dbutils.fs.rm(self.checkpoint_base, True)
        print("Done")

# COMMAND ----------


