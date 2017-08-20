from sklearn.datasets import load_iris
from sklearn.ensemble import RandomForestClassifier
import pandas as pd
import numpy as np


iris = load_iris()
df = pd.DataFrame(iris.data, columns=iris.feature_names)

df['is_train'] = np.random.uniform(0, 1, len(df)) <= .75
train, test = df[df['is_train']==True], df[df['is_train']==False]

features = df.columns[:4]

features

# Create a random forest classifier. By convention, clf means 'classifier'
clf = RandomForestClassifier(n_jobs=2)

# Train the classifier to take the training features and learn how they relate
# to the training y (the species)
clf.fit(train[features], y)

clf.predict(test[features])

pd.crosstab(test['species'], preds, rownames=['Actual Species'], colnames=['Predicted Species'])