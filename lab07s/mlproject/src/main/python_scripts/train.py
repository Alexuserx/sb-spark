#!/opt/anaconda/envs/bd9/bin/python3

import numpy as np
import pandas as pd
import pickle
import base64
import sys
import json

from sklearn.linear_model import LogisticRegression
from scipy.sparse import coo_matrix


data, rows, cols = [], [], []
vocab_size = None
num_rows = 0
labels = []

for i, line in enumerate(sys.stdin):
    parsed = json.loads(line)
    indices = parsed['features']['indices']
    values = parsed['features']['values']
    label = int(parsed['label'])

    if vocab_size is None:
        vocab_size = parsed['features']['size']
    num_rows += 1

    cols.extend(indices)
    rows.extend([i] * len(indices))
    data.extend(values)
    labels.append(label)


matrix = coo_matrix((data, (rows, cols)), shape=(num_rows, vocab_size))

model = LogisticRegression()
model.fit(X=matrix,y=labels)
model_string = base64.b64encode(pickle.dumps(model)).decode('utf-8')

print(model_string)