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
            df_new = dataframe.select(upper(col('city')).alias("City"),dataframe.state_id,upper(dataframe.state_name).alias("State_name"),upper(dataframe.county_name).alias("County_name"),dataframe.population)
            logger.warning("OLTP/OLAP - data processing of 'parquet' file and renaming the columns...")
            
        elif file_format == 'csv' :
            df_new = dataframe.select(dataframe["npi"].alias("presc_id"), dataframe["nppes_provider_last_org_name"].alias("presc_lname"), dataframe["nppes_provider_first_name"].alias("presc_fname"), dataframe["nppes_provider_city"].alias("presc_city"), dataframe["nppes_provider_state"].alias("presc_state"), dataframe["specialty_description"].alias("description"), dataframe["drug_name"], dataframe["total_claim_count"].alias("claim_count"), dataframe["total_day_supply"], dataframe["total_drug_cost"], dataframe["years_of_exp"])
            
            logger.warning("Adding column named country...")
            df_new = df_new.withColumn("Country_Name",lit("USA"))
            
            logger.warning("Converting years of experience from string to int and removing '=' from the data points")
            df_new = df_new.withColumn("years_of_exp",regexp_replace(col("years_of_exp"),r'^=',''))
            df_new = df_new.withColumn("years_of_exp",col('years_of_exp').cast('int'))
            
            logger.warning("Concating First_name and last_name")
            df_new = df_new.withColumn("presc_fullname",concat_ws(' ',"presc_lname","presc_fname"))
            
            logger.warning("Dropping first_name and last_name...")
            df_new = df_new.drop("presc_lname","presc_fname")
            
            logger.warning("Taking Null value count from each column")
            # Using list comprehension technique to to get the count Null values in each column
            df_na = df_new.select([count(when (isnan(c) | col(c).isNull(), c)).alias(c) for c in df_new.columns])
            df_na.show()
            
            df_new = df_new.na.drop()
            
            df_na = df_new.select([count(when (isnan(c) | col(c).isNull(), c)).alias(c) for c in df_new.columns])
            df_na.show()
    
    except Exception as err :
        logger.error(f"Unable to process the {file_format} dataframe due to ==>", str(err))
        
        raise
    
    else:
        logger.warning(f"The {file_format} dataframe has been successfully processed..")
        
        return df_new
    