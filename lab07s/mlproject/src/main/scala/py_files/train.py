#!/opt/anaconda/envs/bd9/bin/python3

import numpy as np
import pandas as pd
import pickle
import sys

from sklearn.linear_model import LogisticRegression

rows = [] #here we keep input data to Dataframe constructor

# iterate over standard input
for line in sys.stdin:
  #parse line into a dict: {"column1": value1, ...}
  print(line)
  line_dict = dict()
  rows.append(line_dict)

#initialize a dataframe from the list
df = pd.DataFrame(rows)

feature_columns = ["x1", "y"]
label_column = "gender_age"

model = LogisticRegression()
model.fit(df[feature_columns], df[label_column])
model_string = base64.b64encode(pickle.dumps(model)).decode('utf-8')

# Output to stdin, so that rdd.pipe() can return the string to pipedRdd.
print(model_string)