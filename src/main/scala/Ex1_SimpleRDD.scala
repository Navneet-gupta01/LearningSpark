import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Ex1_SimpleRDD {
  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("Ex1_SimpleRDD").setMaster("local[4]") // set up a local Spark environment using four threads.
    val sc = new SparkContext(conf)

    // put some data in an RDD
    // Resilient Distributed Dataset, or RDD. This is the data structure on which all Spark computing takes place.

    val numbers = 1 to 10

    val numbersRDD: RDD[Int] = sc.parallelize(numbers, 4 )  // take a regular Scala data structure and turn it into an RDD using the parallelize method on a SparkContext
    println("Print each element of the original RDD")
    numbersRDD.foreach(println) //out of order printing
    // The foreach method causes the println loops to be run in parallel on each of the four partitions.
    // Not only will those four print loops start in random order, but their outputs will be interleaved with each other
    // It's Not that actual order of the elements in RDD has been lost.

    // trivially operate on the numbers
    val stillAnRDD = numbersRDD.map(n => n.toDouble / 10) //This transformation is run independently on each partition with requiring data transfer between the partitions.

    // get the data back out
    val nowAnArray = stillAnRDD.collect()   // The next line gathers all the results into a regular Scala array => Order should be Not Lost.
    // interesting how the array comes out sorted but the RDD didn't
    println("Now print each element of the transformed array")
    nowAnArray.foreach(println)

    // explore RDD properties
    val partitions = stillAnRDD.glom() //RDD's 'glom' method to see what's in the individual partitions of 'stillAnRDD' and print the results as in the following code.
    println("We _should_ have 4 partitions")
    println(partitions.count())
    partitions.foreach(a => {
      println("Partition contents:" +
        a.foldLeft("")((s, e) => s + " " + e))
    })
  }
}
