# -*- coding: utf-8 -*-
import numpy as np
import csv

plots = np.array([[2, 10], [2, 5], [8, 4], [5, 8], [7, 5], [6, 4], [1, 2], [4, 9]])
labels = np.array([f"({x[0]}, {x[1]})" for x in plots])
centers = np.array([[2, 5], [5, 8], [4, 9]])

distances = []
for [xi, yi] in centers:
    line = []
    for [xj, yj] in plots:
        line.append(round(np.sqrt(np.square(xi - xj) + np.square(yi - yj)), 4))
    distances.append(line)

print(labels)
for line in distances:
    print(line)

with open('distances.csv', 'w') as f:
    write = csv.writer(f)
    write.writerow(labels)
    write.writerows(distances)