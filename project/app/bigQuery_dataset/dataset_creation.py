import logging
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

import google.cloud.bigquery
from app.constants import PROJECT_ID
import google.cloud.bigquery as bq
import pandas as pd

# Set up logging
logging.basicConfig(level=logging.INFO,
                     format='%(asctime)s - %(levelname)s - %(message)s',
                     datefmt='%Y-%m-%d %H:%M:%S')

classes_df = pd.read_csv("./classes.csv")
image_labels_df = pd.read_csv("./image-labels.csv")
relations = pd.read_csv("./relations.csv")


def instantiate_table(bq_client: google.cloud.bigquery.Client, table_name: str):
    new_table = PROJECT_ID + '.openimages.' + table_name
    logging.info("Deleting the table {} if already exists".format(table_name))
    bq_client.delete_table(new_table, not_found_ok=True)
    logging.info("Instantiate a new table: {}".format(table_name))
    table = bq.Table(new_table)
    return table


def create_classes_table(bq_client: google.cloud.bigquery.Client, df: pd.DataFrame):
    table = instantiate_table(bq_client, 'classes')
    table.schema = (
        bq.SchemaField('Label', 'STRING'),
        bq.SchemaField('Description', 'STRING')
    )
    logging.info("Create the table classes")
    client.create_table(table, exists_ok=True)
    logging.info("Loading data into classes table")
    client.load_table_from_dataframe(destination=table, dataframe=df)


def create_image_labels_table(bq_client: google.cloud.bigquery.Client, df: pd.DataFrame):
    table = instantiate_table(bq_client, 'image_labels')
    table.schema = (
        bq.SchemaField('ImageId', 'STRING'),
        bq.SchemaField('Label', 'STRING')
    )
    logging.info("Create the table image_labels")
    client.create_table(table)
    logging.info("Loading data into image_labels table")
    client.load_table_from_dataframe(destination=table, dataframe=df)


def create_relations_table(bq_client: google.cloud.bigquery.Client, df: pd.DataFrame):
    table = instantiate_table(bq_client, 'relations')
    table.schema = (
        bq.SchemaField('ImageId', 'STRING'),
        bq.SchemaField('Label1', 'STRING'),
        bq.SchemaField('Relation', 'STRING'),
        bq.SchemaField('Label2', 'STRING')
    )
    logging.info("Create the table relations")
    client.create_table(table)
    logging.info("Loading data into relations table")
    client.load_table_from_dataframe(destination= table, dataframe=df)

if __name__ == "__main__":
    client = bq.Client(project=PROJECT_ID)
    dataset = client.create_dataset('openimages', exists_ok=True)
    create_classes_table(client, classes_df)
    create_image_labels_table(client, image_labels_df)
    create_relations_table(client, relations)

