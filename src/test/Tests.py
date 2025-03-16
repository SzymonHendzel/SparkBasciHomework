# Import necessary libraries for unit testing and PySpark
import unittest  # For creating unit tests
import sys  # For system-specific parameters and functions
import os  # For accessing environment variables
import pygeohash  # For geohashing functionality
from pyspark.sql import SparkSession  # For creating a Spark session
from pyspark.testing import assertDataFrameEqual  # For DataFrame comparison in tests

# Add the path to the main module to the system path for imports
sys.path.append("src\main\python")
from main import get_cordinates, fill_cordinate_na, create_geohash_column  # Import functions to be tested

# Set environment variables for PySpark execution
os.environ['PYSPARK_PYTHON'] = r'C:\ProgramData\Anaconda3\envs\daftacademy-ds\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\ProgramData\Anaconda3\envs\daftacademy-ds\python.exe'
os.environ['GEO_API_KEY'] = 'b59a7ea5f53b475da68a7b36fec49bf6'  # Set a mock Geo API key

# Create a Spark session for testing
spark = SparkSession.builder.master("local").appName("PySpark Unit Tests").getOrCreate()
spark.sparkContext.addPyFile(r'E:\DataEnginerring EPAM\Spark\m06_sparkbasics_python_azure\src\main\python\main.py')  # Add main.py to the Spark context

# Define a test case class for testing the get_cordinates function
class TestGetCoordinates(unittest.TestCase):

    def test_get_cordinates_success(self):
        # Test case for successful geocoding
        country = "United Kingdom"
        city = "London"
        address = "EC1M 5RF"
        result = get_cordinates(country, city, address)  # Call the function

        self.assertEqual(result, (51.522673, -0.102739))  # Assert expected coordinates

    def test_get_cordinates_failure(self):
        # Test case for geocoding failure (invalid input)
        country = "Dummy"
        city = "Data"
        address = "Some Address"
        result = get_cordinates(country, city, address)  # Call the function

        self.assertEqual(result, (None, None))  # Assert that no coordinates are returned

    def test_get_cordinates_empty_values(self):
        # Test case for empty input values
        country = ""
        city = ""
        address = ""
        result = get_cordinates(country, city, address)  # Call the function

        self.assertEqual(result, (None, None))  # Assert that no coordinates are returned

# Define a test case class for testing the fill_cordinate_na function
class TestFillCordinateNa(unittest.TestCase):

    def test_fill_na_Latitude(self):
        # Test case for filling missing latitude values
        correct_data_frame = spark.createDataFrame(
            [
                (1, "name1", "United Kingdom", "London", "EC1M 5RF", 51.522673, -0.102739),
                (2, "name2", "United Kingdom", "London", "EC1M 5RF", 1.0, 1.0),  
            ],
            ["id", "Name", "Country", "City", "Address", "Latitude", "Longitude"] 
        )
        
        data_frame_to_test = spark.createDataFrame(
            [
                (1, "name1", "United Kingdom", "London", "EC1M 5RF", None, -0.102739),
                (2, "name2", "United Kingdom", "London", "EC1M 5RF", 1.0, 1.0),   
            ],
            ["id", "Name", "Country", "City", "Address", "Latitude", "Longitude"] 
        )

        data_frame_to_compare = fill_cordinate_na(data_frame_to_test)  # Call the function to fill missing data
        assertDataFrameEqual(data_frame_to_compare, correct_data_frame)  # Assert that the DataFrames are equal

    def test_fill_na_Longitude(self):
        # Test case for filling missing longitude values
        correct_data_frame = spark.createDataFrame(
            [
                (1, "name1","United Kingdom","London","EC1M 5RF",51.522673,-0.102739),
                (2, "name2","United Kingdom","London","EC1M 5RF",1.0,1.0),  
            ],
            ["id", "Name","Country","City","Address","Latitude","Longitude"] 
        )
        
        data_frame_to_test = spark.createDataFrame(
            [
                (1, "name1","United Kingdom","London","EC1M 5RF",51.522673,None),
                (2, "name2","United Kingdom","London","EC1M 5RF",1.0,1.0),   
            ],
            ["id", "Name","Country","City","Address","Latitude","Longitude"] 
        )

        data_frame_to_compare = fill_cordinate_na(data_frame_to_test)  # Call the function to fill missing data
        assertDataFrameEqual(data_frame_to_compare, correct_data_frame)  # Assert that the DataFrames are equal

    def test_fill_na_Longitude_Latitude(self):
        # Test case for filling both missing latitude and longitude values
        correct_data_frame = spark.createDataFrame(
            [
                (1, "name1","United Kingdom","London","EC1M 5RF",51.522673,-0.102739),
                (2, "name2","United Kingdom","London","EC1M 5RF",1.0,1.0),  
            ],
            ["id", "Name","Country","City","Address","Latitude","Longitude"] 
        )
        
        data_frame_to_test = spark.createDataFrame(
            [
                (1, "name1","United Kingdom","London","EC1M 5RF",None,None),
                (2, "name2","United Kingdom","London","EC1M 5RF",1.0,1.0),   
            ],
            ["id", "Name","Country","City","Address","Latitude","Longitude"] 
        )

        data_frame_to_compare = fill_cordinate_na(data_frame_to_test)  # Call the function to fill missing data
        assertDataFrameEqual(data_frame_to_compare, correct_data_frame)  # Assert that the DataFrames are equal

    def test_do_not_change_filled_data(self):
        # Test case to ensure already filled data remains unchanged
        correct_data_frame = spark.createDataFrame(
            [
                (1, "name1","United Kingdom","London","EC1M 5RF",-1.0,-0.123),
                (2, "name2","United Kingdom","London","EC1M 5RF",1.0,1.0),  
            ],
            ["id", "Name","Country","City","Address","Latitude","Longitude"] 
        )
        
        data_frame_to_test = spark.createDataFrame(
            [
                (1, "name1","United Kingdom","London","EC1M 5RF",-1.0,-0.123),
                (2, "name2","United Kingdom","London","EC1M 5RF",1.0,1.0),   
            ],
            ["id", "Name","Country","City","Address","Latitude","Longitude"] 
        )

        data_frame_to_compare = fill_cordinate_na(data_frame_to_test)  # Call the function to fill missing data
        assertDataFrameEqual(data_frame_to_compare, correct_data_frame)  # Assert that the DataFrames are equal
    
    
        
# Define a test case class for testing the create_geohash_column function
class TestCreateGeohashColumn(unittest.TestCase):

    def test_add_geohash_column(self):
        # Test case for adding a GeoHash column based on latitude and longitude
        correct_data_frame = spark.createDataFrame(
            [
                (1,"name1",'United Kingdom','London','EC1M 5RF',12.231,-0.123, pygeohash.encode(12.231,-0.123,4)),
                (2,"name2",'United Kingdom','London','EC1M 5RF',1.0,1.0, pygeohash.encode(1.0,1.0,4)),  
            ],
            ["id", 'Name','Country','City','Address','Latitude','Longitude','GeoHash'] 
        )
        
        test_data_frame = spark.createDataFrame(
            [
                (1,"name1",'United Kingdom','London','EC1M 5RF',12.231,-0.123),
                (2,"name2",'United Kingdom','London','EC1M 5RF',1.0,1.0),  
            ],
            ["id",'Name','Country','City','Address','Latitude','Longitude'] 
        )
        
        data_frame_to_compare = create_geohash_column(test_data_frame,"Latitude",'Longitude')  # Call the function to add GeoHash column
        assertDataFrameEqual(data_frame_to_compare, correct_data_frame)  # Assert that the DataFrames are equal
        

    def test_add_geohash_column_large_values(self):
        correct_data_frame = spark.createDataFrame(
            [
                (1,"name1",'United Kingdom','London','EC1M 5RF',90.0,180.0, pygeohash.encode(90.0,180.0,4)),
                (2,"name2",'United Kingdom','London','EC1M 5RF',-90.0,-180.0, pygeohash.encode(-90.0,-180.0,4)),  
            ],
            ["id",'Name','Country','City','Address','Latitude','Longitude','GeoHash'] 
        )
        
        test_data_frame = spark.createDataFrame(
            [
                (1,"name1",'United Kingdom','London','EC1M 5RF',90.0,180.0),
                (2,"name2",'United Kingdom','London','EC1M 5RF',-90.0,-180.0),  
            ],
            ["id",'Name','Country','City','Address','Latitude','Longitude'] 
        )
        
        data_frame_to_compare = create_geohash_column(test_data_frame,"Latitude",'Longitude')
        assertDataFrameEqual(data_frame_to_compare, correct_data_frame)

    def test_add_geohash_column_no_coordinates(self):
        correct_data_frame = spark.createDataFrame(
            [
                (1,"name1",'United Kingdom','London','EC1M 5RF',None,None,None),
                (2,"name2",'United Kingdom','London','EC1M 5RF',-90.0,-180.0, pygeohash.encode(-90.0,-180.0,4)),  
            ],
            ["id",'Name','Country','City','Address','Latitude','Longitude','GeoHash'] 
        )
        
        test_data_frame = spark.createDataFrame(
            [
                (1,"name1",'United Kingdom','London','EC1M 5RF',None,None),
                (2,"name2",'United Kingdom','London','EC1M 5RF',-90.0,-180.0),  
            ],
            ["id",'Name','Country','City','Address','Latitude','Longitude'] 
        )
        
        data_frame_to_compare = create_geohash_column(test_data_frame,"Latitude",'Longitude')
        assertDataFrameEqual(data_frame_to_compare, correct_data_frame)
# Main block to run all tests when executed directly
if __name__ == '__main__':
    unittest.main()