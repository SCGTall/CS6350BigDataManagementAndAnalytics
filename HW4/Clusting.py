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


def cal_distance(a, b):
    return np.sqrt(np.square(a[0] - b[0]) + np.square(a[1] - b[1]))


def get_min_index(x):
    try:
        index = 0
        min_e = x[0]
        for i in range(1, len(x)):
            e = x[i]
            if e < min_e:
                min_e = e
                index = i
        return index
    except:
        return 0


def save(x, _record):
    row = []
    for e in x:
        if isinstance(e, str):
            row.append(e)
        elif isinstance(e, float):
            row.append(f"{round(e, 4)}")
        else:
            row.append("Null")
    print(row)
    _record.append(row)
    return


def list_equal(c1, c2):
    if len(c1) != len(c2):
        return False
    for i in range(len(c1)):
        if isinstance(c1[i], list) and isinstance(c2[i], list):
            if not list_equal(c1[i], c2[i]):
                return False
        else:
            if c1[i] != c2[i]:
                return False
    return True


def clusting_func(_plots, _centers, _record):
    clusters = [[], [], []]
    epoch = 1
    while True:
        print(f"Epoch {epoch}")
        distances = []
        # calculate distance
        for center in _centers:
            distance = []
            for plot in _plots:
                distance.append(cal_distance(center, plot))
            distances.append(distance)
        distances_arr = np.array(distances)
        # save in record
        save([""] + [to_label(x) for x in _plots], _record)
        for i in range(len(_centers)):
            save([to_label(_centers[i])] + distances[i], _record)
        # calculate clusters
        new_clusters = [[], [], []]
        for i in range(len(_plots)):
            plot = _plots[i]
            cluster_index = get_min_index(distances_arr[:, i].flatten())
            new_clusters[cluster_index].append(plot.tolist())
        if list_equal(clusters, new_clusters):
            print("Clusters unchange. Finish and terminate.")
            return
        clusters = new_clusters
        # calculate new centers
        new_centers = _centers.copy()
        for i in range(len(clusters)):
            cluster = clusters[i]
            if len(cluster) == 0:
                print("Lost cluster. Early terminate.")
                return
            cluster_arr = np.array(cluster)
            new_centers[i][0] = np.average(cluster_arr[:, 0].flatten())
            new_centers[i][1] = np.average(cluster_arr[:, 1].flatten())
        _centers = new_centers
        # next turn
        epoch += 1


record = []
plots = np.array([[2, 10], [2, 5], [8, 4], [5, 8], [7, 5], [6, 4], [1, 2], [4, 9]])
max_epoch = 10
centers = [[2, 5], [5, 8], [4, 9]]
clusting_func(plots, centers, record)

with open('distances.csv', 'w') as f:
    write = csv.writer(f)
    write.writerows(record)

print("End mission.")
