# Bagged Decision Trees for Classification
import pandas as pd
import numpy as np
from sklearn import model_selection
from sklearn.ensemble import BaggingClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import confusion_matrix
from sklearn.ensemble import RandomForestClassifier

url = "/home/thanos/Dropbox/Code/Hermes/Hermes/Models/Datasets/mclBlastProtein_failure"
#names = ['preg', 'plas', 'pres', 'skin', 'test', 'mass', 'pedi', 'age', 'class']
dataframe = pd.read_csv(url)
array = dataframe.values
size = 1
X = array[:,0:size]
Y = array[:,size]
seed = 7

kfold = model_selection.KFold(n_splits=10, random_state=seed)
cart = DecisionTreeClassifier()
num_trees = 100
model = RandomForestClassifier(n_jobs=2)
results = model_selection.cross_val_score(model, X, Y, cv=kfold)
print(results.mean())




model.fit(X,Y)




preds = model.predict(X)

print confusion_matrix(preds, Y)
print model.feature_importances_
#print model.score(X,Y)
#print cls

#print X.tolist()
#print results
#print(results.mean())

#predicted=model.predict(X)
#print predicted
