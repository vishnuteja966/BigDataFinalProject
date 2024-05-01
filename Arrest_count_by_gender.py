import pandas as pd
import matplotlib.pyplot as plt
from neo4j import GraphDatabase
import yaml

class Neo4jConnector:
    def __init__(self, uri, user, password):
        self.neo_uri = uri
        self.neo_user = user
        self.neo_password = password
        self.neo_driver = GraphDatabase.driver(self.neo_uri, auth=(self.neo_user, self.neo_password))

    def retrieve_data(self, query):
        with self.neo_driver.session() as session:
            result = session.run(query)
            return pd.DataFrame([record.values() for record in result], columns=result.keys())

class DataProcessor:
    def __init__(self, connector):
        self.connector = connector

    def retrieve_data_from_neo4j(self, query):
        return self.connector.retrieve_data(query)

    def plot_bar_chart(self, data, x_label, y_label, title):
        plt.figure(figsize=(8, 6))
        plt.bar(data[x_label], data[y_label], color='skyblue')
        plt.title(title)
        plt.xlabel(x_label)
        plt.ylabel(y_label)
        plt.xticks(rotation=0)
        plt.tight_layout()
        plt.show()

def main():
    # Load configuration from YAML file
    with open('config.yaml', 'r') as config_file:
        config = yaml.safe_load(config_file)

    # Access Neo4j connection details
    neo_uri = config['neo4j']['uri']
    neo_user = config['neo4j']['user']
    neo_password = config['neo4j']['password']

    # Initialize Neo4j connector
    connector = Neo4jConnector(neo_uri, neo_user, neo_password)

    # Initialize DataProcessor
    processor = DataProcessor(connector)

    # Neo4j query to find the sex with the most arrests
    query = """
        MATCH (n) 
        WHERE n.sex_code IS NOT NULL
        RETURN n.sex_code AS sex_code, count(*) AS arrest_count
    """

    # Retrieve data from Neo4j
    data = processor.retrieve_data_from_neo4j(query)

    # Plotting
    processor.plot_bar_chart(data, 'sex_code', 'arrest_count', 'Sex with the Most Arrests')

if __name__ == "__main__":
    main()
