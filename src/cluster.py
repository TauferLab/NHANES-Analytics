## Copyright (c)
##    2017 by The University of Delaware
##    Contributors: Michael Wyatt
##    Affiliation: Global Computing Laboratory, Michela Taufer PI
##    Url: http://gcl.cis.udel.edu/, https://github.com/TauferLab
##
## All rights reserved.
##
## Redistribution and use in source and binary forms, with or without
## modification, are permitted provided that the following conditions are met:
##
##    1. Redistributions of source code must retain the above copyright notice,
##    this list of conditions and the following disclaimer.
##
##    2. Redistributions in binary form must reproduce the above copyright
##    notice, this list of conditions and the following disclaimer in the
##    documentation and/or other materials provided with the distribution.
##
##    3. If this code is used to create a published work, one or both of the
##    following papers must be cited.
##
##            M. Wyatt, T. Johnston, M. Papas, and M. Taufer.  Development of a
##            Scalable Method for Creating Food Groups Using the NHANES Dataset
##            and MapReduce.  In Proceedings of the ACM Bioinformatics and
##            Computational Biology Conference (BCB), pp. 1 - 10. Seattle, WA,
##            USA. October 2 - 4, 2016.
##
##    4.  Permission of the PI must be obtained before this software is used
##    for commercial purposes.  (Contact: taufer@acm.org)
##
## THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
## AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
## IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
## ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
## LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
## CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
## SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
## INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
## CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
## ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
## POSSIBILITY OF SUCH DAMAGE.

from pyspark import SparkContext, SparkConf
import argparse
import numpy as np
from spark_lib.ml.DBSCAN import DBSCAN

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--data', type=str, required=True,
            help='Data file to load')
    parser.add_argument('-o', '--out', type=str, required=True,
            help='Location to output clusters')
    parser.add_argument('-e', '--epsilon', type=float, required=False, default=1,
            help='Radius at which DBSCAN will look for neighbors of a point')
    parser.add_argument('-p', '--minpts', type=int, required=False, default=5,
            help='Minimum number of neighbors to be a core point in DBSCAN')
    parser.add_argument('-m', '--metric', type=str, required=False,
            default='euclidean', help='Metric for distance measure')
    args = parser.parse_args()

    conf = SparkConf()
    sc = SparkContext(conf=conf)

    # Load data
    data = sc.pickleFile(args.data)

    print 'epsilon: %f, minpts: %d' % (args.epsilon, args.minpts)
    print 'data points: %d' % data.count()

    # Run DBSCAN
    clusters = DBSCAN(sc, data, epsilon=args.epsilon,
            minpts=args.minpts, metric=args.metric)

    # Get number of clusters
    clust_count = clusters.map(lambda x: (x[1], x[0]))
    clust_count = clust_count.reduceByKey(lambda x,y: x).count()
    print 'DBSCAN found %d clusters' % clust_count
    print 'Clustered Points: %d' % clusters.count()

    # Join clusters information with original data (for analysis)
    clusters = clusters.join(data)

    print 'saving clusters'
    clusters.saveAsPickleFile(args.out)
