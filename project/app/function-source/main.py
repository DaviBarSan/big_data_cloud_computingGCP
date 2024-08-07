import functions_framework
import wget
import google.cloud.storage as storage
from zipfile import ZipFile
import os
from PIL import Image
import tensorflow as tf
import numpy as np



@functions_framework.http
def hello_http(request):
    print("The model is classify images of these labels:\nMake sure your image url contain one of them.")
    request_json = request.get_json()
    image_url = request_json.get('image_url')
    image = wget.download(image_url, "image.jpg")

    download_model()
    score_response = score_image()

    os.remove(image)
    return score_response


def download_model():
    storage_client = storage.Client(project="project-up202310061-bdcc")
    vertex_bucket = storage_client.bucket("vertex-ai-project-up202310061")
    blob = vertex_bucket.blob("model-1696837812239728640/tflite/2024-04-07T15:25:06.403931Z/model.tflite")

    blob.download_to_filename("model.tflite")
    print("Model downloaded to as model.tflite")

    with ZipFile("model.tflite", 'r') as zip_ref:
      zip_ref.extractall("./")

def score_image():
    model_file = os.path.join('./', 'model.tflite')
    label_file = os.path.join('./', 'dict.txt')
    image = os.path.join('./', 'image.jpg')

    tf_classifier = Model(model_file, label_file)

    results = tf_classifier.classify(image, min_confidence=0.01)
    build_response = ""
    for i,r in enumerate(results):
        build_response += ('{},{},{},{:.2f}\n'.format(image, i+1, r['label'], float(r['confidence'])))

    return build_response

class Model:

    def __init__(self, model_file, dict_file):
        with open(dict_file, 'r') as f:
            self.labels = [line.strip().replace('_', ' ') for line in f.readlines()]
        self.interpreter = tf.lite.Interpreter(model_path=model_file)
        self.interpreter.allocate_tensors()
        self.input_details = self.interpreter.get_input_details()
        self.output_details = self.interpreter.get_output_details()
        self.floating_model = self.input_details[0]['dtype'] == np.float32
        self.height = self.input_details[0]['shape'][1]
        self.width = self.input_details[0]['shape'][2]

    def classify(self, file, min_confidence):
        with Image.open(file).convert('RGB').resize((self.width, self.height)) as img:
            input_data = np.expand_dims(img, axis=0)
            if self.floating_model:
                input_data = (np.float32(input_data) - 127.5) / 127.5
            self.interpreter.set_tensor(self.input_details[0]['index'], input_data)
            self.interpreter.invoke()
            output_data = self.interpreter.get_tensor(self.output_details[0]['index'])
            model_results = np.squeeze(output_data)
            top_categories = model_results.argsort()[::-1]
            results = []
            for i in top_categories:
                if self.floating_model:
                    confidence = float(model_results[i])
                else:
                    confidence = float(model_results[i] / 255.0)
                if min_confidence != None and confidence < min_confidence:
                    break
                results.append(dict(label=self.labels[i], confidence='%.2f'%confidence))
            return results
