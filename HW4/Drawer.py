# -*- coding: utf-8 -*-
import numpy as np
import matplotlib.pyplot as plt

plots = np.array([[2, 10], [2, 5], [8, 4], [5, 8], [7, 5], [6, 4], [1, 2], [4, 9]])
x_list = plots[:,0]
y_list = plots[:,1]
plt.scatter(x_list, y_list, marker='o', color='m')
plt.title("Scatter Diagram")
plt.axis("equal")
plt.axis([min(x_list)-0.5, max(x_list) + 1, min(y_list)-0.5, max(y_list) + 1])
for [x, y] in plots:
    plt.annotate(f"({x}, {y})", xy=(x, y), xytext=(5, 5), textcoords='offset points')

plt.show()