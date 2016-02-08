import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

/**
	`users.txt`:
	Customer ID, Customer full name, email list, phone list 

	`donotcall.txt`:
	 phone numbers that should not be used in the campaign
	 
	`transactions.txt`:
	Customer ID, transaction amount, transaction date
*/

object Simple {
  def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("Spark Test App")
    val sc = new SparkContext(conf)

    // broadcast the do-not-call list
    val doNotCallBC = sc.broadcast(sc.textFile("donotcall.txt").collect)
  
  	val users = sc.textFile("users.txt").map(_.split(";")).
  	  // filter out users without any phone number
  	  filter(_.size == 4).
  	  // remove users' numbers according to do not call list,
  	  // and change RDD format to prepare for a join operation
  	  map(x => (x(0), (x(1), (x(3).split(",").toSet -- doNotCallBC.value.toSet)))).
  	  // remove users with no phone numbers left
  	  filter(_._2._2.nonEmpty)
  	
  	val transactions = sc.textFile("transactions.txt").map(_.split(";")).
  	  // filter by 2015 year
  	  filter(_(2).startsWith("2015")).
  	  // convert amount to BigDecimal
  	  map(x => (x(0), BigDecimal(x(1).drop(1)))).
  	  // calculate total amount per user 
  	  reduceByKey{_ + _}
  	
  	// join users with transactions
  	val topUsers = users.join(transactions).
  	  // get top users based on the total amount value
  	  takeOrdered(100)(Ordering[BigDecimal].reverse.on(_._2._2))
  	
  	// output results in the requested format
  	val formattedResult = topUsers.map(x => "%s, %s, %s, %s".format(
  	    x._1, 
  	    x._2._1._1, 
  	    x._2._1._2.mkString("\"", ", ", "\""), 
  	    x._2._2.toString))
  	scala.tools.nsc.io.File("results-simple.csv").writeAll(formattedResult.mkString("\n"))
  	
  }
}
