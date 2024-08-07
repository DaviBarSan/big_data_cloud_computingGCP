# Imports
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

from constants import PROJECT_ID
import flask
import logging
import os
import tfmodel
from google.cloud import bigquery
from google.cloud import storage

# Set up logging
logging.basicConfig(level=logging.INFO,
                     format='%(asctime)s - %(levelname)s - %(message)s',
                     datefmt='%Y-%m-%d %H:%M:%S')

PROJECT = os.environ.get('GOOGLE_CLOUD_PROJECT')
logging.info('Google Cloud project is {}'.format(PROJECT))

# Initialisation
logging.info('Initialising app')
app = flask.Flask(__name__)

logging.info('Initialising BigQuery client')
BQ_CLIENT = bigquery.Client()

BUCKET_NAME = PROJECT + '.appspot.com'
logging.info('Initialising access to storage bucket {}'.format(BUCKET_NAME))
APP_BUCKET = storage.Client().bucket(BUCKET_NAME)

logging.info('Initialising TensorFlow classifier')
TF_CLASSIFIER = tfmodel.Model(
    app.root_path + "/static/tflite/model.tflite",
    app.root_path + "/static/tflite/dict.txt"
)
logging.info('Initialisation complete')

# End-point implementation
@app.route('/')
def index():
    return flask.render_template('index.html')

@app.route('/classes')
def classes():
    results = BQ_CLIENT.query(
    '''
        Select Description, COUNT(*) AS NumImages
        FROM `bdcc24project.openimages.image_labels`
        JOIN `bdcc24project.openimages.classes` USING(Label)
        GROUP BY Description
        ORDER BY Description
    ''').result()
    logging.info('classes: results={}'.format(results.total_rows))
    data = dict(results=results)
    return flask.render_template('classes.html', data=data)

@app.route('/relations')
def relations():
    results = BQ_CLIENT.query(
        '''
            select Relation, count(Relation) 
            from `project-up202310061-bdcc.openimages.relations`
            group by Relation;
        '''
    ).result()
    data = dict(results=results)
    logging.info('relations: results={}'.format(results.total_rows))
    return flask.render_template('relations.html', data=data)



@app.route('/image_info')
def image_info():
    image_id = flask.request.args.get('image_id')
    results = BQ_CLIENT.query(
        '''
            SELECT
              rlt.ImageId,
              STRING_AGG(DISTINCT CONCAT(cls1.description,' ', rlt.relation,' ', cls2.description)) AS relation,
              STRING_AGG(DISTINCT all_desc.all_descriptions) AS descriptions
            FROM
              `project-up202310061-bdcc.openimages.relations` rlt
            INNER JOIN
              `project-up202310061-bdcc.openimages.classes` cls1
            ON
              rlt.Label1 = cls1.Label
            INNER JOIN
              `project-up202310061-bdcc.openimages.classes` cls2
            ON
              rlt.Label2 = cls2.Label
            INNER JOIN (
              SELECT
                cls.Description AS all_descriptions,
                img.ImageId AS ImgId_1
              FROM
                `project-up202310061-bdcc.openimages.image_labels` img
              JOIN
                `project-up202310061-bdcc.openimages.classes` cls
              USING
                (Label)) AS all_desc
            ON
              rlt.ImageId = all_desc.ImgId_1
            WHERE  rlt.ImageId = '{img_id}'
            GROUP BY
            rlt.ImageId
        '''.format(img_id=image_id)
    ).result()


    data = dict(results=results)

    return flask.render_template('image_info.html', data=data)

@app.route('/image_search')
def image_search():
    description = flask.request.args.get('description', default='')
    image_limit = flask.request.args.get('image_limit', default=10, type=int)
    results = BQ_CLIENT.query(
        '''
        SELECT description, imageId from 
        `project-up202310061-bdcc.openimages.classes`
        join `project-up202310061-bdcc.openimages.image_labels` using (Label)
        where description = '{img_description}'
        limit {limit}
        '''.format(img_description=description, limit=image_limit)
    ).result()
    data = dict(results=results)
    logging.info('image_search: results={}'.format(results.total_rows))
    return flask.render_template('image_search.html', data=data)

@app.route('/relation_search')
def relation_search():
    class1 = flask.request.args.get('class1', default='%')
    relation = flask.request.args.get('relation', default='%')
    class2 = flask.request.args.get('class2', default='%')
    image_limit = flask.request.args.get('image_limit', default=10, type=int)
    results = BQ_CLIENT.query(
        '''
            select rlt.ImageId,
              dsc1.description as description_image1,
              rlt.relation,
              dsc2.description as description_image2,
              from `project-up202310061-bdcc.openimages.relations` rlt
              inner join `project-up202310061-bdcc.openimages.classes` dsc1
              on rlt.Label1 = dsc1.Label
              inner join `project-up202310061-bdcc.openimages.classes` dsc2
              on rlt.Label2  = dsc2.Label
              where rlt.relation like '{relation_parameter}'
              and dsc1.description like '{desc_image1}'
              and dsc2.description like '{desc_image2}'
              limit {limit};
        '''.format(relation_parameter=relation,desc_image1=class1, desc_image2=class2, limit=image_limit,)
    ).result()
    data = dict(results=results)
    return flask.render_template('relations_search.html', data=data)


@app.route('/image_classify_classes')
def image_classify_classes():
    with open(app.root_path + "/static/tflite/dict.txt", 'r') as f:
        data = dict(results=sorted(list(f)))
        return flask.render_template('image_classify_classes.html', data=data)
 
@app.route('/image_classify', methods=['POST'])
def image_classify():
    files = flask.request.files.getlist('files')
    min_confidence = flask.request.form.get('min_confidence', default=0.25, type=float)
    results = []
    if len(files) > 1 or files[0].filename != '':
        for file in files:
            classifications = TF_CLASSIFIER.classify(file, min_confidence)
            blob = storage.Blob(file.filename, APP_BUCKET)
            blob.upload_from_file(file, blob, content_type=file.mimetype)
            blob.make_public()
            logging.info('image_classify: filename={} blob={} classifications={}'\
                .format(file.filename,blob.name,classifications))
            results.append(dict(bucket=APP_BUCKET,
                                filename=file.filename,
                                classifications=classifications))
    
    data = dict(bucket_name=APP_BUCKET.name, 
                min_confidence=min_confidence, 
                results=results)
    return flask.render_template('image_classify.html', data=data)



if __name__ == '__main__':
    # When invoked as a program.
    logging.info('Starting app')
    app.run(host='127.0.0.1', port=8080, debug=True)


