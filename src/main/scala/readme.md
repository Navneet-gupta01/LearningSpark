# Ex1_SimpleRDD


```scala
  // set up a local Spark environment using four threads.
  val conf = new SparkConf().setAppName("Ex1_SimpleRDD").setMaster("local[4]")
  val sc = new SparkContext(conf)
```

* Above Code will set up a local Spark Environment using Four threads.

```scala
  sc.parallelize(numbers, 4 )
```

* Above code takes a regular Scala data structure(numbers: Scala Range) and turn it into an RDD using the parallelize method on a SparkContext

* RDD methods map/foreach run transfromations/function independently on each partition

* RDD's .collect method gathers all the results into a regular Scala array and Will be in sync with order of Input values. 