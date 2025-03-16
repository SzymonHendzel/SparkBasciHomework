# Import necessary libraries
from pyspark.sql import SparkSession  # For creating a Spark session
from pyspark import SparkConf  # For configuring Spark settings
from pyspark.sql.functions import col, udf, avg, first  # For DataFrame operations
from pyspark.sql.types import StructType, StructField, DoubleType  # For defining schema
from opencage.geocoder import OpenCageGeocode  # For geocoding addresses
import pygeohash  # For encoding geographical coordinates into geohashes
import os  # For accessing environment variables

# Retrieve API keys and storage account details from environment variables
GEO_API_KEY = os.environ.get("GEO_API_KEY")
STROAGE_ACCOUNT_NAME_IN = os.environ.get("STORAGE_ACCOUNT_NAME_IN")
STROAGE_ACCOUNT_NAME_OUT = os.environ.get("STORAGE_ACCOUNT_NAME_OUT")
STORAGE_KEY_IN = os.environ.get("STORAGE_KEY_IN")
STORAGE_KEY_OUT = os.environ.get("STORAGE_KEY_OUT")
CONTAINER_NAME_IN = os.environ.get("CONTAINER_NAME_IN")
CONTAINER_NAME_OUT = os.environ.get("CONTAINER_NAME_OUT")

# Define storage paths for input and output data
STORAGE_PATH_IN = f'wasbs://{CONTAINER_NAME_IN}@{STROAGE_ACCOUNT_NAME_IN}.blob.core.windows.net'
STORAGE_PATH_OUT = f'wasbs://{CONTAINER_NAME_OUT}@{STROAGE_ACCOUNT_NAME_OUT}.blob.core.windows.net'

# Define a function to create a Spark session with necessary configurations
def create_session():
    conf = SparkConf().setAppName("Spark ETL")\
            .set('spark.jars.packages', 
              'org.apache.hadoop:hadoop-azure:3.3.1,' 
              'com.microsoft.azure:azure-storage:7.0.0')\
            .set(f"spark.hadoop.fs.azure.account.key.{STROAGE_ACCOUNT_NAME_IN}.blob.core.windows.net", STORAGE_KEY_IN) \
            .set(f"spark.hadoop.fs.azure.account.key.{STROAGE_ACCOUNT_NAME_OUT}.blob.core.windows.net", STORAGE_KEY_OUT)  
    
    spark = SparkSession.builder.config(conf=conf).getOrCreate()  # Create and return Spark session
    return spark

# Define a function to retrieve geographical coordinates based on country, city, and address
def get_cordinates(country: str, city: str, address: str):
    geocoder = OpenCageGeocode(GEO_API_KEY)  # Initialize the geocoder with the API key
    results = geocoder.geocode(f"{address}, {city}, {country}")  # Geocode the address

    if results:
        return (results[0]['geometry']['lat'], results[0]['geometry']['lng'])  # Return latitude and longitude if found
    else:
        return (None, None)  # Return None if no results are found

# Function to fill missing coordinates in the DataFrame
def fill_cordinate_na(df):
    df_with_null = df.where((col("Latitude").isNull()) | (col("Longitude").isNull()))  # Filter rows with null coordinates
    df_without_null = df.where((col("Latitude").isNotNull()) & (col("Longitude").isNotNull()))  # Filter rows with valid coordinates

    # Define schema for the user-defined function (UDF)
    schema = StructType([
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True)
    ])

    # Create UDF to apply it on DataFrame
    udf_get_coordinates = udf(get_cordinates, schema)
    
    df_with_coordinates = df_with_null.withColumn(
        "Coordinates",
        udf_get_coordinates("Country", "City", "Address")  # Apply UDF to get coordinates
    )

    # Select relevant columns from the DataFrame
    df_with_coordinates = df_with_coordinates.select(
        "Id", "Name", "Country", "City", "Address",
        df_with_coordinates.Coordinates.lat.alias("Latitude"),
        df_with_coordinates.Coordinates.lon.alias("Longitude")
    )

    return df_without_null.union(df_with_coordinates)  # Combine DataFrames with and without nulls

# Function to generate a geohash from latitude and longitude
def generate_geohash(lat, long):
    generate = True
    try:
        x = float(lat)  # Attempt to convert latitude to float
        y = float(long)  # Attempt to convert longitude to float
    except:
        generate = False  # If conversion fails, set flag to False

    if generate:
        return pygeohash.encode(float(x), float(y), 4)  # Return geohash with precision of 4 characters
    else:
        return None  # Return None if conversion failed

# Function to create a new column in DataFrame containing geohashes
def create_geohash_column(df, lat_name, long_name):
    udf_geohash_encode = udf(generate_geohash)  # Create UDF for generating geohashes 
    return df.withColumn("GeoHash", udf_geohash_encode(col(lat_name), col(long_name)))  # Add GeoHash column to DataFrame

# Main execution block of the script
if __name__ == '__main__':
    spark = create_session()  # Create Spark session
    
    # Read hotel data from CSV file into DataFrame
    df_hotel = spark.read.option("header", "true").csv(f"{STORAGE_PATH_IN}/m06sparkbasics/hotels/")
    
    # Read weather data from Parquet file into DataFrame with recursive file lookup enabled
    df_weather= spark.read.option("recursiveFileLookup", "true") \
                      .parquet(f"{STORAGE_PATH_IN}/m06sparkbasics/weather/") 
    
    # Prepare hotel data by filling missing coordinates and generating geohashes
    df_hotel = fill_cordinate_na(df_hotel)
    df_hotel = create_geohash_column(df_hotel,'Latitude','Longitude')
    
    # Prepare weather data by generating geohashes and aggregating temperature data by date and GeoHash
    df_weather = create_geohash_column(df_weather,'lat','lng')
    
    df_weather = df_weather.groupBy(["wthr_date","GeoHash"]).agg(
                                     avg("avg_tmpr_f").alias("avg_tmpr_f"),  # Calculate average temperature in Fahrenheit
                                     avg("avg_tmpr_c").alias("avg_tmpr_c"),  # Calculate average temperature in Celsius
                                     first("lat").alias("lat"),              # Get first latitude value for each group
                                     first("lng").alias("lng"))              # Get first longitude value for each group
   
    # Combine hotel and weather data based on matching GeoHashes and save the result as Parquet files partitioned by date
    joined_df = df_weather.join(df_hotel, df_hotel.GeoHash == df_weather.GeoHash, 'left')
    
    joined_df = joined_df.drop(df_hotel.GeoHash)  # Drop duplicate GeoHash column from hotel data
    
    joined_df.write.partitionBy("wthr_date").parquet(f"{STORAGE_PATH_OUT}/data", mode="append")  # Save combined DataFrame
    
    spark.stop()  # Stop the Spark session when done
