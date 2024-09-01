import sys
import os
import env_var
from spark_session import get_spark_object
from Validate import get_current_date, print_schema
import logging
import logging.config
from ingest import ingest_data
from Data_processing import data_processing



logging.config.fileConfig('logging.config')
logger = logging.getLogger('root')
logger.setLevel(logging.DEBUG)

# function to create spark session/object
def main():
    
    try :
        logging.info('Your in main method...')
        #print(env_var.header)
        #print(env_var.src_path)
        logging.info('calling spark object...')
        # Takes the environment and appname from the env_var.py file to create a sparksession using     spark_session.py file...
        #print('Creating spark Session/object...')
        spark = get_spark_object(env_var.envn,env_var.appName)
        #print('Created Spark Session... ',spark)
        # Validating the newly created spark object using validate.py file
        logging.info('Validating the Spark object')
        get_current_date(spark)
        
    except Exception as exp :
        logging.error("An error occurred... while calling main() please check ===> ",str(exp))
        sys.exit(1)
        
    return spark
    
# Function to clear the log file before each excution.
def clear_log_file(log_file):
    with open(log_file, 'w'):
        pass

    
# function to get the csv file_name list
def get_sourc_files(path,file_type):
    # for loop to get all the file names of all the csv files present in the src_path directory
    for file_nm in os.listdir(path):
        # assigning the parameter "file_type" to file_format variable
        file_format = file_type
        if file_nm.endswith('.parquet') and file_type == 'parquet' : # takes files having ".parquet" at the end of the file
            header = 'NA'
            inferschema = 'NA'
            # concating source file path the file name
            path_file = path + '\\' + file_nm
        elif file_nm.endswith('.csv') and file_type == 'csv' : # takes files having ".csv" at the end of the file
            header = env_var.header
            inferschema = env_var.InferSchema
            # concating source file path the file name
            path_file = path + '\\' + file_nm
    # prints the file name the is getting read
    logging.info("reading files which is of ==> {}".format(file_format))
    # returns
    return file_format, header, inferschema, path_file
            
if __name__ == '__main__':
    
    # Clear the log file before starting the new runtime
    clear_log_file('application.log')

    out_spark = main() # creates spark session/object
    
    # Reads the files from source path and assigns base parameters using the file_type
    parq_file_format, parq_header, parq_inferschema, parq_path_file = get_sourc_files(env_var.src_path,'parquet')
    # Creates a dataframe using the ingest_data method from ingest file.
    df_city = ingest_data(spark=out_spark,file_path=parq_path_file,file_format=parq_file_format,header=parq_header,inferschema=parq_inferschema)
    # Displays the first five records of the dataframe.
    #df_city.show()
    
    # creating dataframe for csv file.
    csv_file_format, csv_header, csv_inferschema, csv_path_file = get_sourc_files(env_var.src_path,'csv')
    # Creates a dataframe using the ingest_data method from ingest file.
    df_medicare = ingest_data(spark=out_spark,file_path=csv_path_file,file_format=csv_file_format,header=csv_header,inferschema=csv_inferschema)
    # Displays the first five records of the dataframe.
    #df_medicare.show()

    # Processing the data of parquet file
    df_par = data_processing(df_city,'parquet')
    #df_par.show()
    # Processing the data of csv file
    df_csv = data_processing(df_medicare,'csv')
    #df_csv.show()
    
    logger.info("Validating the Schema of dataframes...")
    df_par.show() # Parquet file data
    df_csv.show() # CSV file data
    
    ###### ending the spark process ##########
    logger.info("Application Done...")
    out_spark.stop()
    
    