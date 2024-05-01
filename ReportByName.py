import pandas as pd
import matplotlib.pyplot as plt
from neo4j import GraphDatabase
import yaml

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

    def retrieve_data(self, query):
        with self.neo_driver.session() as session:
            result = session.run(query)
            return pd.DataFrame([record.values() for record in result], columns=result.keys())

class DataProcessor:
    def __init__(self, config_file):
        self.config = self.load_config(config_file)

    def load_config(self, config_file):
        with open(config_file, 'r') as f:
            return yaml.safe_load(f)

    def retrieve_data_from_neo4j(self, query):
        connector = Neo4jConnector('config.yaml')
        return connector.retrieve_data(query)

    def plot_data(self, data):
        plt.figure(figsize=(10, 6))
        plt.bar(data['area_name'], data['report_count'], color='skyblue')
        plt.title('Report Count by Area')
        plt.xlabel('Area Name')
        plt.ylabel('Report Count')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.show()

def main():
    processor = DataProcessor('config.yaml')

    # Neo4j query to get report count in each area
    query = """
        MATCH (n) 
        WHERE n.area_name IS NOT NULL
        WITH n.area_name AS area_name, count(n) AS report_count
        RETURN area_name, report_count
    """

    # Retrieve data from Neo4j
    data = processor.retrieve_data_from_neo4j(query)

    # Plotting
    processor.plot_data(data)

if __name__ == "__main__":
    main()
