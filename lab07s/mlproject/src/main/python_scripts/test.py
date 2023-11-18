#!/opt/anaconda/envs/bd9/bin/python3

import numpy as np
import pandas as pd
import pickle
import base64
import sys
import json

from sklearn.linear_model import LogisticRegression
from scipy.sparse import coo_matrix


model = pickle.loads(
    base64.b64decode(
        open("lab07.model").read().encode('utf-8')
    )
)

data, rows, cols = [], [], []
vocab_size = None
num_rows = 0

for i, line in enumerate(sys.stdin):
    parsed = json.loads(line)
    indices = parsed['features']['indices']
    values = parsed['features']['values']

    if vocab_size is None:
        vocab_size = parsed['features']['size']
    num_rows += 1

    cols.extend(indices)
    rows.extend([i] * len(indices))
    data.extend(values)


try:
    matrix = coo_matrix((data, (rows, cols)), shape=(num_rows, vocab_size))
except Exception as e:
    print("FUCKED UP MATRIX: {}".format(e))

try:
    preds = model.predict_proba(matrix)
except Exception as e:
    print("FUCKED UP MODEL: {}".format(e))

# for pred in preds:
#     print(list(pred))