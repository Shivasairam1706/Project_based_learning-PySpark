# This file is used for transforming the data based on business requirements.
import logging
import logging.config
from pyspark.sql.functions import *

logging.config.fileConfig('logging.config')
logger = logging.getLogger('Data_processing')

def data_processing(dataframe,file_format):
    # Data Cleaning
    try:
        logger.warning("Data Cleaning process has started...")
        logger.warning("Converting columns like City, State into Upper Case and limiting the columns")
        # Condition for pulling selective columns from dataframe based on the dataframe source type...
        if file_format == 'parquet':
            df_new = dataframe.select(upper(col('city')),dataframe.state_id,upper(dataframe.state_name),upper(dataframe.county_name),dataframe.population)
            logger.warning("OLTP/OLAP - data processing of 'parquet' file and renaming the columns...")
        elif file_format == 'csv' :
            df_new = dataframe.select(dataframe["npi"].alias("presc_id"), dataframe["nppes_provider_last_org_name"].alias("presc_lname"), dataframe["nppes_provider_first_name"].alias("presc_fname"), dataframe["nppes_provider_city"].alias("presc_city"), dataframe["nppes_provider_state"].alias("presc_state"), dataframe["specialty_description"].alias("description"), dataframe["drug_name"], dataframe["total_claim_count"].alias("claim_count"), dataframe["total_day_supply"], dataframe["total_drug_cost"], dataframe["years_of_exp"])
    
    except Exception as err :
        logger.error(f"Unable to process the {file_format} dataframe due to ==>", str(err))
        
        raise
    
    else:
        logger.warning(f"The {file_format} dataframe has been successfully processed..")
        
        return df_new