# Java Universal Recommendation Template


The Universal Recommender (UR) is a Cooccurrence type that creates correlators from several user actions, events, or profile information and performs the recommendations query with a Search Engine. It also supports item properties for filtering and boosting recommendations. This allows users to make use of any part of their user's clickstream or even profile and context information in making recommendations. TBD: several forms of popularity type backfill and content-based correlators for content based recommendations. Also filters on property date ranges. With these additions it will more closely live up to the name "Universal"

See the scala-version github project for more information: [UR template scala](https://github.com/PredictionIO/template-scala-parallel-universal-recommendation)

This is a Cornell CS 5152 team project. 

## Versions

### v0.1.0

 - converting the engine from scala to java

### Known issues

 - see the github [issues list](https://github.com/PredictionIO/template-scala-parallel-universal-recommendation/issues)

## References

 * Other documentation of the algorithm is [here](http://mahout.apache.org/users/algorithms/intro-cooccurrence-spark.html)
 * A free ebook, which talks about the general idea: [Practical Machine Learning](https://www.mapr.com/practical-machine-learning).
 * A slide deck, which talks about mixing actions and other correlator types, including content-based ones: [Creating a Unified Recommender](http://www.slideshare.net/pferrel/unified-recommender-39986309?ref=http://occamsmachete.com/ml/)
 * Two blog posts: What's New in Recommenders: part [#1](http://occamsmachete.com/ml/2014/08/11/mahout-on-spark-whats-new-in-recommenders/) [#2](http://occamsmachete.com/ml/2014/09/09/mahout-on-spark-whats-new-in-recommenders-part-2/)
 * A post describing the log-likelihood ratio: [Surprise and Coincidence](http://tdunning.blogspot.com/2008/03/surprise-and-coincidence.html) LLR is used to reduce noise in the data while keeping the calculations O(n) complexity.
 
#License
This Software is licensed under the Apache Software Foundation version 2 licence found here: http://www.apache.org/licenses/LICENSE-2.0

