from pyspark import SparkContext, SparkConf
import numpy as np
from spark_lib.ml.cluster_analysis import _InterClusterDist, _IntraClusterDist


if __name__ == '__main__':
    conf = SparkConf()
    sc = SparkContext(conf=conf)

    cluster_dir = '../data/cluster/'
    data = sc.pickleFile(cluster_dir)

    # Broadcast clusters
    cluster_bc = data.map(lambda x: x[1])
    cluster_bc = cluster_bc.collect()
    cluster_bc = sc.broadcast(cluster_bc)

    # Get data in correct format
    data = data.map(lambda x: (x[1][0], x[1]))

    # Get intracluster distances
    ICD = data.mapValues(lambda x: _IntraClusterDist(x, cluster_bc, 'euclidean'))
    ICD = ICD.reduceByKey(lambda x,y: max(x,y))
    ICD = ICD.map(lambda x: x[1])
    ICD = ICD.collect()
    print('intra distance cluster values:')
    print('mean:', np.mean(ICD))
    print('max:', max(ICD))

    # Get intercluster distances
    ICD = data.mapValues(lambda x: _InterClusterDist(x, cluster_bc, 'euclidean'))
    ICD = ICD.reduceByKey(lambda x,y: max(x,y))
    ICD = ICD.map(lambda x: x[1])
    ICD = ICD.collect()
    print('inter distance cluster values:')
    print('mean:', np.mean(ICD))
    print('max:', max(ICD))
