from pyspark.sql import SparkSession
from neo4j import GraphDatabase
import yaml
import os

class Neo4jConnector:
    def __init__(self, config_file):
        self.config = self.load_config(config_file)
        self.neo_uri = self.config['neo4j']['uri']
        self.neo_user = self.config['neo4j']['user']
        self.neo_password = self.config['neo4j']['password']
        self.neo_driver = GraphDatabase.driver(self.neo_uri, auth=(self.neo_user, self.neo_password))

    def load_config(self, config_file):
        with open(config_file, 'r') as f:
            return yaml.safe_load(f)

    def insert_data(self, nodes):
        with self.neo_driver.session() as session:
            for node in nodes.collect():
                session.run("""
                    CREATE (r:Report {
                        id: $id,
                        report_type: $report_type,
                        arrest_date: $arrest_date,
                        time: $time,
                        area_id: $area_id,
                        area_name: $area_name,
                        reporting_district: $reporting_district,
                        age: $age,
                        sex_code: $sex_code,
                        descent_code: $descent_code,
                        charge_group_code: $charge_group_code,
                        charge_group_description: $charge_group_description,
                        arrest_type_code: $arrest_type_code,
                        charge: $charge,
                        charge_description: $charge_description,
                        disposition_description: $disposition_description,
                        address: $address,
                        cross_street: $cross_street,
                        lat: $lat,
                        lon: $lon
                    })
                """, node.asDict())

class DataProcessor:
    def __init__(self, config_file):
        self.config = self.load_config(config_file)
        self.spark = SparkSession.builder.appName("CSV to Neo4j").getOrCreate()

    def load_config(self, config_file):
        with open(config_file, 'r') as f:
            return yaml.safe_load(f)

    def read_csv(self):
        current_path = os.getcwd()
        csv_path = current_path + "\Arrest_Data_from_2020_to_Present.csv"
        return self.spark.read.csv(csv_path, header=True, inferSchema=True)

    def transform_data(self, df):
        return df.selectExpr(
            "`Report ID` AS id",
            "`Report Type` AS report_type",
            "`Arrest Date` AS arrest_date",
            "Time AS time",
            "`Area ID` AS area_id",
            "`Area Name` AS area_name",
            "`Reporting District` AS reporting_district",
            "Age AS age",
            "`Sex Code` AS sex_code",
            "`Descent Code` AS descent_code",
            "`Charge Group Code` AS charge_group_code",
            "`Charge Group Description` AS charge_group_description",
            "`Arrest Type Code` AS arrest_type_code",
            "Charge AS charge",
            "`Charge Description` AS charge_description",
            "`Disposition Description` AS disposition_description",
            "Address AS address",
            "`Cross Street` AS cross_street",
            "LAT AS lat",
            "LON AS lon"
        )

    def stop_spark(self):
        self.spark.stop()

def main():
    # Load configuration from YAML file
    connector = Neo4jConnector('config.yaml')
    processor = DataProcessor('config.yaml')

    # Read CSV data using Spark
    df = processor.read_csv()

    # Transform data into a format suitable for Neo4j
    nodes = processor.transform_data(df)

    # Call the function to insert data into Neo4j
    connector.insert_data(nodes)

    # Close SparkSession
    processor.stop_spark()

if __name__ == "__main__":
    main()
