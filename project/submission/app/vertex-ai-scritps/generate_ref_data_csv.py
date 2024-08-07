import logging

from google.cloud.bigquery._helpers import _get_bigquery_host
# Imports
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

from constants import PROJECT_ID, VERTEX_AI_BUCKET_NAME
import pandas as pd
import wget
import os
from google.cloud import bigquery
from google.cloud import storage

class RefDataVertexGenerator():
    def __init__(self):
        self.query_result = None
        self.image_uris_df = None
        self.bigquery_client = bigquery.Client(project=PROJECT_ID)
        self.storage_client = STORAGE_CLIENT = storage.Client(project=PROJECT_ID)
        self.vertex_bucket = STORAGE_CLIENT.bucket(VERTEX_AI_BUCKET_NAME)
        self.labels = None

    def query_uris_from_self_labels(self):
        logging.info("quering images")
        self.__query_image_ref(self.labels)
        self.__append_image_uri()

    def load_images_to_vertex_bucket(self):
        logging.info("loading images to {vertex_bucket_name}...".format(vertex_bucket_name=VERTEX_AI_BUCKET_NAME))
        image_url = "https://storage.googleapis.com/bdcc_open_images_dataset/images/"
        images_df = self.image_uris_df
        images_df['image_url'] = image_url + images_df['img_id'] + '.jpg'

        load_counter = 1
        for index, row in images_df.iterrows():
            image = wget.download(row['image_url'])
            blob = self.vertex_bucket.blob("images/{image_id}.jpg".format(image_id=row['img_id']))
            blob.upload_from_filename(image, content_type='image/jpg')
            logging.info("loaded {load_counter} from {total_count}...".format(load_counter=load_counter, total_count=len(images_df)))
            load_counter += 1

            os.remove(image)
    def load_ref_id_to_vertex_bucket(self):
        logging.info("loading reference data to {vertex_bucket_name}...".format(vertex_bucket_name=VERTEX_AI_BUCKET_NAME))
        ref_data_gen.image_uris_df[['img_uri', 'label']].to_csv("./dataset_vertex_ai.csv", index=False, header=False)
        blob = self.vertex_bucket.blob("dataset_vertex_ai.csv")
        blob.upload_from_filename("./dataset_vertex_ai.csv")


    def set_labels(self,labels_list:list):
        self.labels = labels_list

    def __query_image_ref(self, labels_query: list):
        logging.info("query started...")
        list_to_str = ','.join(['"' + word + '"' for word in labels_query])

        query_image_ids = self.bigquery_client.query('''
            with row_lim as (
              select lbs.ImageId as img_id, cls.Description as label, 
              row_number() over(partition by Label) as row_num
              from `project-up202310061-bdcc.openimages.image_labels` lbs
              inner join `project-up202310061-bdcc.openimages.classes` cls using(Label)
              where cls.Description in ({labels}))
               select row_lim.img_id as img_id, max(row_lim.label) as label
               from row_lim
               where row_lim.row_num <= 110
               group by row_lim.img_id
        '''.format(labels=list_to_str)).result()
        self.query_result = query_image_ids
        logging.info("query ended!")


    def __append_image_uri(self):
        logging.info("creating images ref data dataframe...")
        self.image_uris_df = bigquery.table.RowIterator.to_dataframe(self.query_result)
        self.image_uris_df['img_uri'] = 'gs://' + VERTEX_AI_BUCKET_NAME + '/images/' + self.image_uris_df['img_id'] + '.jpg'


if __name__ == "__main__":
    ref_data_gen = RefDataVertexGenerator()
    labels = ["Wine glass", "Motorcycle", "Hamburger", "Cart",
              "Airplane", "Cat", "Rifle", "Broccoli", "Otter", "Wheelchair"]
    ref_data_gen.set_labels(labels)
    ref_data_gen.query_uris_from_self_labels()
    ref_data_gen.load_images_to_vertex_bucket()
    ref_data_gen.load_ref_id_to_vertex_bucket()


