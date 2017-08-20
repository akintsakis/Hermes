# Bagged Decision Trees for Classification
import pandas
import numpy as np
from sklearn import model_selection
from sklearn.svm import SVR
from sklearn.model_selection import train_test_split
url = "/home/thanos/Dropbox/Code/Hermes/Hermes/Models/Datasets/mclBlastProtein_runtime"
#names = ['preg', 'plas', 'pres', 'skin', 'test', 'mass', 'pedi', 'age', 'class']
dataframe = pandas.read_csv(url, header = None)
array = dataframe.values
X = array[:,0:10]
Y = array[:,10]
X_trn, X_tst, Y_trn, Y_tst = train_test_split(X, Y, test_size=0.3, random_state=42)
print "train set is of size"+str(X_trn.shape)
print "test set is of size"+str(X_tst.shape)
seed = 7

#kfold = model_selection.KFold(n_splits=10, random_state=seed)
#svr_model = SVR(kernel='rbf', C=1e3, gamma=0.01)
svr_model = SVR(kernel='linear', C=1e3)
#svr_model = SVR(kernel='poly', C=1e3, degree=2)


#regr_rf.fit(X, Y)
svr_model.fit(X_trn, Y_trn)

error = svr_model.predict(X_tst) - Y_tst

print max(np.abs((Y_tst - svr_model.predict(X_tst)) / Y_tst))
print min(np.abs((Y_tst - svr_model.predict(X_tst)) / Y_tst))
abs_diff=np.abs((Y_tst - svr_model.predict(X_tst)) / Y_tst)

print "MAPE: " + str(np.mean(abs_diff) * 100)
abs_diff_filtered=abs_diff[np.where(abs_diff<2)]
print abs_diff_filtered.shape
print abs_diff.shape
print max(abs_diff_filtered)
print min(abs_diff_filtered)
print "MAPE excluding outliers: " + str(np.mean(abs_diff_filtered) * 100)


#print results
#print(results.mean())

predicted=svr_model.predict(X)

predictions = predicted.tolist()
real = Y.tolist()

#for i in range (0, len(predictions)):
    #print str(predictions[i]) + " " + str(real[i])

