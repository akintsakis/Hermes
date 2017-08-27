from flask import Flask, request
import rf_regression
import rf_classifier
import json

app = Flask(__name__)
models = dict()
modelPredType = dict()

@app.route("/")
def main():
    return "Welcome!"

@app.route('/build_model/<uuid>', methods=['GET'])
def build_model(uuid):
    request_json = json.loads(uuid)
    fileUrl = request_json["fileUrl"].replace(";","/")
    if(request_json["predictionType"] == 'regression'):
        models[request_json["modelId"]] = rf_regression.train_model_all_data(fileUrl, request_json["modelId"])
        modelPredType[request_json["modelId"]] = 'regression'
    elif(request_json["predictionType"] == 'classification'):
        models[request_json["modelId"]] = rf_classifier.train_model_all_data(fileUrl, request_json["modelId"])
        modelPredType[request_json["modelId"]] = 'classification'
    #print uuid
    return ""

@app.route('/classify_sample/<uuid>', methods=['GET'])
def classify_sample(uuid):
    request_json = json.loads(uuid)
    #print request_json["modelId"]
    #print models
    model = models[request_json["modelId"]]
    value = "0"
    if(modelPredType[request_json["modelId"]] == 'regression'):
        value = rf_regression.classify_instance(model, request_json["featuresVectorCsv"])
    elif(modelPredType[request_json["modelId"]] == 'classification'):
        value = rf_classifier.classify_instance(model, request_json["featuresVectorCsv"])
    return str(value)

if __name__ == "__main__":
    app.run()
