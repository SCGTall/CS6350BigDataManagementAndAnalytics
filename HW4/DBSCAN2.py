# -*- coding: utf-8 -*-
import numpy as np
import matplotlib.pyplot as plt

plots = np.array([[2, 10], [2, 5], [8, 4], [5, 8], [7, 5], [6, 4], [1, 2], [4, 9]])
x_list = plots[:,0]
y_list = plots[:,1]
plt.scatter(x_list, y_list, marker='o', color='m')


def draw_circle(_x0, _y0, _r, _color):
    theta = np.linspace(0, 2 * np.pi, 200)
    x = _x0 + np.cos(theta) * _r
    y = _y0 + np.sin(theta) * _r
    plt.plot(x, y, color=_color)


clusters = [
    np.array([[8, 4], [7, 5], [6, 4]]),
    np.array([[2, 10], [5, 8], [4, 9]]),
    np.array([[2, 5], [1, 2]]),
]
colors = ["r", "b", "g", "b"]
r = np.sqrt(10)  # Epsilon
for (i, c) in enumerate(clusters):
    draw_circle(np.average(c[:, 0].flatten()), np.average(c[:, 1].flatten()), r, colors[i])
for plot in plots:
    plt.annotate(f"({plot[0]}, {plot[1]})", xy=(plot[0], plot[1]), xytext=(5, 5), textcoords='offset points')
plt.title(f"DBSCAN Result (Epsilon = {round(r, 4)})")
plt.axis("equal")
plt.axis([min(x_list)-3.8, max(x_list) + 4.3, min(y_list)-3.8, max(y_list) + 4.3])

plt.show()