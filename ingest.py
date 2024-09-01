# to ingest the file data into target/Sink

import logging
import logging.config

logging.config.fileConfig('logging.config')
logger = logging.getLogger('ingest')

def  ingest_data(spark,file_path,file_format,header,inferschema):
    try :
        logger.warning('File loading has started...')
        
        if file_format == 'parquet':
            df = spark.read.format(file_format).load(file_path)
        
        elif file_format == 'csv' :
            df = spark.read.format(file_format).options(header = header, inferschema = inferschema).load(file_path)
        # Displays the count of the newly loaded dataframe
        logger.warning(f'Total no.of records loaded into dataframe from file : {df.count()}')
        
        return df
    
    except Exception as err :
        logger.error(f"Unable to ingest data from {file_path} due to: {err}")
        
        raise
    
    else :
        logger.warning('Ingest process completed and DataFrame (df) created successfully...')



