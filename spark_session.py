from pyspark.sql import SparkSession
import logging.config

logging.config.fileConfig('logging.config')
logger = logging.getLogger('spark_session')
logger.setLevel(logging.DEBUG)


# gobal variable to check the environment
Environments = ('DEV','SIT','UAT')

def get_spark_object(envn, appName):
    # Try to execute the below code if there is any issue it will skip the try and moves to except...
    try:
        logger.info('get_spark_object menthod has started...')
        if envn in Environments :
            master = 'local'
    
        else :
            master = 'Yarn'
        
        logger.info('master is {}'.format(master))
        # Creates a spark session using the "master"-variable and "appName"-argument...
        spark = SparkSession.builder.master(master).appName(appName).getOrCreate()
        
    
    # Executes the below in case of an issue with the above try...
    except Exception as exp:
        logger.error('An error occured in get_spark_object menthod... please check ===>', str(exp))
        
        raise
    
    else :
        logger.info('Spark session/object created successfully...')
    
    # returns the created sparksession.
    return spark








