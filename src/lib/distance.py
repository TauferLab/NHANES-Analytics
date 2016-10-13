import numpy as np

def _Euclidean(x, y):
    return np.sum((x - y) ** 2) ** 0.5

def _Cosine(x, y):
    dist = np.dot(x, y)
    dist = dist / ((np.sum(x ** 2) ** 0.5) * (np.sum(y ** 2) ** 0.5))
    dist = 1 - dist
    return dist

# Computer distance between two points
def PPDistance(x, y, metric='euclidean'):
    x = np.array(x, dtype=float)
    y = np.array(y, dtype=float)
    if metric == 'euclidean':
        return _Euclidean(x, y)
    if metric == 'cosine':
        return _Cosine(x, y)
