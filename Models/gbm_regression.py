# Bagged Decision Trees for Classification
import pandas
import numpy as np
from sklearn import model_selection
from sklearn.ensemble import BaggingClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingRegressor

url = "/home/thanos/Dropbox/Code/Hermes/Hermes/Models/Datasets/blastProteinDistributed_runtime"
#names = ['preg', 'plas', 'pres', 'skin', 'test', 'mass', 'pedi', 'age', 'class']
dataframe = pandas.read_csv(url, header = None)
#dataframe = pandas.read_csv("/home/thanos/Desktop/housing.data", delim_whitespace=True, header=None)
array = dataframe.values

print array.shape[1]
num_features = array.shape[1] - 1
X = array[:,0:num_features]
Y = array[:,num_features]
X_trn, X_tst, Y_trn, Y_tst = train_test_split(X, Y, test_size=0.3, random_state=42)
print "train set is of size"+str(X_trn.shape)
print "test set is of size"+str(X_tst.shape)
seed = 7

#kfold = model_selection.KFold(n_splits=10, random_state=seed)
cart = DecisionTreeClassifier()

regr_rf = GradientBoostingRegressor(n_estimators=12000, max_depth=10, learning_rate = .01)
#results = model_selection.cross_val_score(model, X, Y, cv=kfold)

#regr_rf.fit(X, Y)
regr_rf.fit(X_trn, Y_trn)

error = regr_rf.predict(X_tst) - Y_tst

print max(np.abs((Y_tst - regr_rf.predict(X_tst)) / Y_tst))
print min(np.abs((Y_tst - regr_rf.predict(X_tst)) / Y_tst))
abs_diff=np.abs((Y_tst - regr_rf.predict(X_tst)) / Y_tst)

print "MAPE: " + str(np.mean(abs_diff) * 100)
abs_diff_filtered=abs_diff[np.where(abs_diff<2)]
print abs_diff_filtered.shape
print abs_diff.shape
print max(abs_diff_filtered)
print min(abs_diff_filtered)
print "MAPE excluding outliers: " + str(np.mean(abs_diff_filtered) * 100)


#print results
#print(results.mean())

predicted=regr_rf.predict(X)

predictions = predicted.tolist()
real = Y.tolist()

#for i in range (0, len(predictions)):
    #print str(predictions[i]) + " " + str(real[i])

