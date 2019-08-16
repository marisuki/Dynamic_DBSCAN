# Recurrence of: Dynamic Density Based Clustering(Tao et.al SIGMOD 2017)

This repo is a recurrence of dynamic DBSCAN for paper: [Dynamic Density Based Clustering](https://dl.acm.org/citation.cfm?id=3064050) with some optimization on the indexing , calculation of core points and the linkage of core points/core cells.


## Process:

THIS REPO IS STILL UNDER BUILDING. Some Bugs may occur!

This repo is based on [Spark Streaming](http://spark.apache.org/streaming/) for dynamic data.

I have finished the main parts of dbscan and it now need to be tested: on performance and accuracy. 

This implementation is based on the [QuadForest](https://github.com/marisuki/QuadForest) for accelerating the process of maintaining the core points/cells. And if you want to use this repo as a mean of clustering dynamic and static data, you'd better use this repo: [QuadForest](https://github.com/marisuki/QuadForest) for testing the parameters: [eps, rho, minPts] for better performance: use the QuadForest/test/UnitTest.java and feed you data demo to cut the error rate.

## Other things:

Others(Theory, Implementation Mertics and Result) will given once I finish the test on this implementation.


## Reference: 

[paper](https://dl.acm.org/citation.cfm?id=3064050) 

Gan, Junhao, and Yufei Tao. "Dynamic density based clustering." Proceedings of the 2017 ACM International Conference on Management of Data. ACM, 2017. 
