rm -rf ../data/processed/*
rm -rf ../data/cluster/*

# This will download all of the NHANES data - but you will need to pick out the
# datafiles which you are required by the food group analysis scripts. (i.e.,
# IFF files for each year)
mkdir ../data/raw_NHANES
./get_data.py ./NHANES_URLS.txt -o ../data/raw_NHANES/ -m

# check out the other options available for preprocessing by running:
# python preprocess.py -h
mkdir ../data/processed
spark-submit preprocess.py -d ../data/raw/ -v -f ../data/features.txt -o ../data/processed
# DBSCAN has 3 options to change, epsilon and minpts (-e and -p) as well as a
# distance metric, which is currently limited to just "euclidean" or "cosine"
mkdir ../data/cluster
spark-submit cluster.py -d ../data/processed -o ../data/cluster -e 1 -p 5 -m euclidean
