import logging.config

logging.config.fileConfig('logging.config')
logger = logging.getLogger('validate')


# function the validate the spark session
def get_current_date(spark):
    try:
        logger.info("started th get_current_date method...")
        output = spark.sql("""select Current_date""")
        # Pyspark is follows lazy evalation to check check the spark object is created or not we are using collect fuction to execute the spark object...
        logger.info("Validating spark object with current date" + str(output.collect()))
    
    except Exception as exp:
        logger.error("An error has occured in get_current_date method ===>",str(exp))
        
        raise
        
    else :
        logger.info("Validatation done... We are good to proceed...")
        

def print_schema(dataframe):
    
    try:
        logger.info("Validating schema method execution...")
        
        df_schema = dataframe.schema.fields
        
        for i in df_schema:
            logger.info(f"\t{i}")
        
    except Exception as err:
        logger.error(f"Unable to validate {dataframe} Schema detailed error msg ===> {err}")
        
        raise
    else:
        logger.info(f"The {dataframe} Schema has been validated successfully")