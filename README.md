# NHANES-Analytics

This repository contains code related to NHANES dietary data analysis for
creating nutrient-driven food groups, as documented in:

M. Wyatt, T. Johnston, M. Papas, and M. Taufer.  Development of a Scalable
Method for Creating Food Groups Using the NHANES Dataset and MapReduce.  In
*Proceedings of the ACM Bioinformatics and Computational Biology Conference
(BCB)*, pp. 1 – 10. Seattle, WA, USA. October 2 – 4, 2016.

# Code

The code runs one Apache Spark and is split into 2 parts:

_Preprocessing_
`./src/preprocess.py` contains the PySpark script for preprocessing the
NHANES dietary data

_Clustering_
`./src/cluster.py` contains the PySpark script for clustering the preprocessed
data

# Running

To run the code, you will need Apache Spark installed.  You can run the code
with the bash script located at `./src/run.sh`

# Data

1 Year of NHANES data is in `./data/`.  This data and more years of NHANES data
can be downloaded from the (CDC
website)[https://www.cdc.gov/nchs/tutorials/nhanes/Preparing/Download/intro.htm]
