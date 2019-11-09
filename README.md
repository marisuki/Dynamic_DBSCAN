# Recurrence of: Dynamic Density Based Clustering(Tao et.al SIGMOD 2017)

This repo is a recurrence of dynamic DBSCAN for paper: [Dynamic Density Based Clustering](https://dl.acm.org/citation.cfm?id=3064050) with some optimization on the indexing, calculation of core points and the linkage of core points/core cells. The related data frames are based on the paper: [DBSCAN Revisited: Mis-Claim, Un-Fixability, and Approximation](https://dl.acm.org/citation.cfm?doid=2723372.2737792) and the reoccurence project is [QuadForest](https://github.com/marisuki/QuadForest). This repo has finished building and is under testing now. Please inform me when errors occurs, I will help to solve that.


## Status:

This repo is based on [Spark Streaming](http://spark.apache.org/streaming/) for dynamic data.

I have finished all algorithms and it now need to be tested: on performance and accuracy. 

This implementation is based on the [QuadForest](https://github.com/marisuki/QuadForest) for accelerating the process of maintaining the core points/cells. And if you want to use this repo as a mean of clustering dynamic and static data. 

## Testing:

From init file, this repo can achieve 640734/684017ms = 0.93 pts/ms (Testing evironment: Intel i7-8700 @3.2GHz, 24GB RAM, JDK-1.8, Scala-2.11, Win10Pro)
For testing, the command ` nc -l -p (port) ` to push data into processing stream.

## Other things:

Others(Theory, Implementation Mertics and Result) will given once I finish the test on this implementation.


## Reference: 

[paper](https://dl.acm.org/citation.cfm?id=3064050) 

Gan, Junhao, and Yufei Tao. "Dynamic density based clustering." Proceedings of the 2017 ACM International Conference on Management of Data. ACM, 2017. 
