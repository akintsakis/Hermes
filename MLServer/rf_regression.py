# Bagged Decision Trees for Classification
import pandas
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
import argparse
import pickle

def classify_instance(model, instance):
	instance_array = instance.split(",")
	x = np.array(instance_array)
	return model.predict(x).tolist()[0]

def train_model_all_data(url, name):
	dataframe = pandas.read_csv(url, header = None)
	array = dataframe.values
	num_features = array.shape[1] - 1
	X = array[:,0:num_features]
	Y = array[:,num_features]
	return train_model(X,Y,name)

def train_model(X,Y, name):
	seed = 7
	regr_rf = RandomForestRegressor(n_estimators=100, max_depth=30, random_state=2, n_jobs=-1)

	regr_rf.fit(X, Y)

	error = regr_rf.predict(X) - Y

	#print max(np.abs((Y - regr_rf.predict(X)) / Y))
	#print min(np.abs((Y - regr_rf.predict(X)) / Y))
	abs_diff=np.abs((Y - regr_rf.predict(X)) / Y)

	print name + " MAPE: " + str(np.mean(abs_diff) * 100)
	abs_diff_filtered=abs_diff[np.where(abs_diff<2)]
	#print abs_diff_filtered.shape
	#print abs_diff.shape
	#print max(abs_diff_filtered)
	#print min(abs_diff_filtered)
	print name + " MAPE excluding outliers: " + str(np.mean(abs_diff_filtered) * 100)
	return regr_rf


#### For testing
# parser = argparse.ArgumentParser()
# parser.add_argument("url")
# args = parser.parse_args()
# url = args.url
#
# dataframe = pandas.read_csv(url, header = None)
# array = dataframe.values
# print array.shape[1]
# num_features = array.shape[1] - 1
# X = array[:,0:num_features]
# Y = array[:,num_features]
# X_trn, X_tst, Y_trn, Y_tst = train_test_split(X, Y, test_size=0.3, random_state=42)
# print "train set is of size"+str(X_trn.shape)
# print "test set is of size"+str(X_tst.shape)
# seed = 7
#
# regr_rf = train_model(X_trn, Y_trn, 'test')
# error = regr_rf.predict(X_tst) - Y_tst
#
# #print max(np.abs((Y_tst - regr_rf.predict(X_tst)) / Y_tst))
# #print min(np.abs((Y_tst - regr_rf.predict(X_tst)) / Y_tst))
# abs_diff=np.abs((Y_tst - regr_rf.predict(X_tst)) / Y_tst)
#
# print "MAPE: " + str(np.mean(abs_diff) * 100)
# abs_diff_filtered=abs_diff[np.where(abs_diff<2)]
# #print abs_diff_filtered.shape
# #print abs_diff.shape
# #print max(abs_diff_filtered)
# #print min(abs_diff_filtered)
# print "MAPE excluding outliers: " + str(np.mean(abs_diff_filtered) * 100)
