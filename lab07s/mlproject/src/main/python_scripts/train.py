#!/opt/anaconda/envs/bd9/bin/python3

import numpy as np
import pandas as pd
import pickle
import base64
import sys
import re

from sklearn.linear_model import LogisticRegression
from scipy.sparse import coo_matrix


data, rows, cols = [], [], []
vocab_size = None
num_rows = 0
labels = []

for i, line in enumerate(sys.stdin):
    (indices, counts) = re.compile(r'\[[0-9.,]+\]').findall(line)
    indices = list(map(int, indices.strip("[").strip("]").split(",")))
    counts = list(map(float, counts.strip("[").strip("]").split(",")))
    splitted = line.split(",")

    if vocab_size is None:
        vocab_size = int(splitted[0].strip("[("))
    num_rows += 1

    cols.extend(indices)
    rows.extend([i] * len(indices))
    data.extend(counts)
    labels.append(int(float(splitted[-1].strip("]\n"))))


matrix = coo_matrix((data, (rows, cols)), shape=(num_rows, vocab_size))

model = LogisticRegression()
model.fit(X=matrix,y=labels)
model_string = base64.b64encode(pickle.dumps(model)).decode('utf-8')

print(model_string)