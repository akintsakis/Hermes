import pandas
import numpy as np
from sklearn.ensemble import BaggingClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import confusion_matrix
from sklearn.ensemble import RandomForestClassifier

from sklearn.model_selection import train_test_split
import argparse
import pickle

def classify_instance(model, instance):
	instance_array = instance.split(",")
	x = np.array(instance_array)
	#print model.predict_proba(x).tolist()[0][0]
	return model.predict_proba(x).tolist()[0][0]

def train_model_all_data(url, name):
	dataframe = pandas.read_csv(url, header = None)
	array = dataframe.values
	num_features = array.shape[1] - 1
	X = array[:,0:num_features]
	Y = array[:,num_features]
	return train_model(X,Y,name)

def train_model(X,Y, name):
	seed = 7
	classifier_rf = RandomForestClassifier(n_estimators=100, max_depth=30, n_jobs=-1)

	classifier_rf.fit(X, Y)
	error = classifier_rf.predict(X) - Y
        preds = classifier_rf.predict(X)
        print confusion_matrix(preds, Y)
        print classifier_rf.score(X,Y)

	return classifier_rf


#### For testing
if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("url")
	args = parser.parse_args()
	url = args.url

	dataframe = pandas.read_csv(url, header = None)
	array = dataframe.values
	print array.shape[1]
	num_features = array.shape[1] - 1
	X = array[:,0:num_features]
	Y = array[:,num_features]
	X_trn, X_tst, Y_trn, Y_tst = train_test_split(X, Y, test_size=0.3, random_state=42)
	print "train set is of size"+str(X_trn.shape)
	print "test set is of size"+str(X_tst.shape)
	seed = 7

	classifier_rf = train_model(X_trn, Y_trn, 'test')
	error = classifier_rf.predict(X_tst) - Y_tst

	preds = classifier_rf.predict(X_tst)
	#print preds.tolist()
	#print classifier_rf.predict_proba(X_tst).tolist()
	print classifier_rf.feature_importances_
	print confusion_matrix(preds, Y_tst)
	print classifier_rf.score(X_tst,Y_tst)
	#print cls
