# -*- coding: utf-8 -*-
import numpy as np
import csv


def relax(x):
    v = x
    if isinstance(x, float):
        v = round(x, 4)
    return str(v)


def to_label(x):
    return f"({relax(x[0])}, {relax(x[1])})"


def cal_distance(p1, p2):
    return np.sqrt(np.square(p1[0] - p2[0]) + np.square(p1[1] - p2[1]))


plots = np.array([[2, 10], [2, 5], [8, 4], [5, 8], [7, 5], [6, 4], [1, 2], [4, 9]])

record = [[""] + [to_label(x) for x in plots]]
for i in range(len(plots)):
    row = [to_label(plots[i])]
    for j in range(len(plots)):
        row.append(round(cal_distance(plots[i], plots[j]), 4))
    record.append(row)

with open('distances2.csv', 'w') as f:
    write = csv.writer(f)
    write.writerows(record)