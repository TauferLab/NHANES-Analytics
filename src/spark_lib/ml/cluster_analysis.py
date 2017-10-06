import sys
sys.path.append('../../')
from lib.distance import PPDistance

# Finds distance to closest other cluster
def _InterClusterDist(x, clusters, metric):
    clusters = clusters.value
    key = x[0]
    val = x[1]
    min_dist = sys.maxint
    for point in clusters:
        if point[0] == key:
            continue
        dist = PPDistance(val, point[1], metric=metric)
        if dist < min_dist:
            min_dist = dist
    return min_dist

# Finds diameter of a cluster
def _IntraClusterDist(x, clusters, metric):
    clusters = clusters.value
    key = x[0]
    val = x[1]
    max_dist = 0
    for point in clusters:
        if point[0] != key:
            continue
        dist = PPDistance(val, point[1], metric=metric)
        if dist > max_dist:
            max_dist = dist
    return max_dist

def DunnIndex(sc, data, metric='euclidean'):
    clusters = data.map(lambda x: x[1])
    clusters = clusters.collect()
    clusters = sc.broadcast(clusters)

    d_min = data.mapValues(lambda x: _InterClusterDist(x, clusters, metric))
    d_min = d_min.values().reduce(lambda x,y: min(x,y))

    d_max = data.mapValues(lambda x: _IntraClusterDist(x, clusters, metric))
    d_max = d_max.values().reduce(lambda x,y: max(x,y))

    return d_min/d_max
