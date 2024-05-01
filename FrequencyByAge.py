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

    def plot_pie_chart(self, data):
        plt.figure(figsize=(8, 8))
        plt.pie(data['frequency'], labels=data['age_group'], autopct='%1.1f%%', startangle=140)
        plt.title('Frequency of Crimes by Age Group')
        plt.axis('equal')
        plt.show()

def main():
    processor = DataProcessor('config.yaml')

    # Neo4j query to get the frequency of age groups
    query = """
        MATCH (n)
        WHERE n.age IS NOT NULL
        WITH n, CASE
            WHEN n.age <= 18 THEN 'Under 18'
            WHEN n.age <= 25 THEN '18-25'
            WHEN n.age <= 35 THEN '26-35'
            WHEN n.age <= 45 THEN '36-45'
            WHEN n.age <= 55 THEN '46-55'
            ELSE 'Over 55'
        END AS age_group
        RETURN age_group, count(n) AS frequency
    """

    # Retrieve data from Neo4j
    data = processor.retrieve_data_from_neo4j(query)

    # Plotting
    processor.plot_pie_chart(data)

if __name__ == "__main__":
    main()
