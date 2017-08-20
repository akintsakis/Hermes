import numpy as np
import pandas
from keras.models import Sequential
from keras.layers import Dense, Dropout, Activation
from keras.wrappers.scikit_learn import KerasRegressor
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import KFold
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from keras import optimizers
from sklearn import preprocessing

# load dataset



url = "/home/thanos/Dropbox/Code/Hermes/Hermes/Models/Datasets/blastProteinDistributed_runtime"
dataframe = pandas.read_csv(url,  header=None)
dataframe = pandas.read_csv("/home/thanos/Desktop/housing.data", delim_whitespace=True, header=None)
dataset = dataframe.values
print dataset.shape
num_features = dataset.shape[1] - 1
# split into input (X) and output (Y) variables
X = dataset[:,0:num_features]
Y = dataset[:,num_features]

X_trn, X_tst, Y_trn, Y_tst = train_test_split(X, Y, test_size=0.2, random_state=42)

scale = StandardScaler()

#X_trn = preprocessing.normalize(X_trn)
#Y_trn = preprocessing.normalize(Y_trn)[0,:]
#X_tst = preprocessing.normalize(X_tst)
#Y_tst = preprocessing.normalize(Y_tst)[0,:]

#X_trn = scale.fit_transform(X_trn)
#X_tst = scale.fit_transform(X_tst)
#Y_trn = scale.fit_transform(Y_trn)
#Y_tst = scale.fit_transform(Y_tst)

# define base model
def baseline_model():
	# create model
        sgd = optimizers.SGD(lr=0.001, decay=1e-6, momentum=0.1, nesterov=True)
	model = Sequential()
	model.add(Dense(20, input_dim=num_features, kernel_initializer='glorot_normal', activation='relu'))
        #model.add(Dropout(0.3))
        #model.add(Dense(2, kernel_initializer='normal', activation='relu'))
        #model.add(Dropout(0.1))
        #model.add(Dense(5, kernel_initializer='normal', activation='relu'))
        #model.add(Dense(5, kernel_initializer='normal', activation='relu'))
	model.add(Dense(1, kernel_initializer='normal'))
	# Compile model
	model.compile(loss='mean_squared_error', optimizer='adam')
	return model


# fix random seed for reproducibility
seed = 5
np.random.seed(seed)
# evaluate model with standardized dataset
estimator = KerasRegressor(build_fn=baseline_model, epochs=50, batch_size=5, verbose=1)
print " ------------- "
estimator.fit(X_trn,Y_trn)

#print estimator.predict(X)

score = estimator.score(X_tst, Y_tst)
print "score MSE: " + str(score)

print "differences tables"

#score = estimator.score(X, Y)
#print score

#kfold = KFold(n_splits=10, random_state=seed)
#results = cross_val_score(estimator, X, Y, cv=kfold)
#print("Results: %.2f (%.2f) MSE" % (results.mean(), results.std()))
#print results.tolist()
#print Y.tolist()


regr_rf = estimator
error = regr_rf.predict(X_tst) - Y_tst
print error

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

print(regr_rf.predict(X_tst).tolist())
print(Y_tst.tolist())
