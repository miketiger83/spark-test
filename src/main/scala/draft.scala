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
	
	Assumptions/Simplifications: 
	# no validation of input data
	# no optimizations. Ex: instead of loading to objects, we could filter out rows first based on substring matches
	# one line code - normally I would split it to several lines to make debugging easier
	# no constants, like separators, year, etc
	# we assume that BigDecimal capacity is enough to hold result  anount of user transactions	
	# phone numbers do not repeat for one user 
	# TODO
	
	Thoughts: 
	IMPORTANT: Thoughts have to be verified on practice
	Having 500K rows in transactions (maybe half of which are 2015) 
	and 25K users (20K of which will have phone numbers)
	=> we want to do filter sources and do calculations on nodes first, do join the latest
*/

object SimpleApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Test App")
    val sc = new SparkContext(conf)
	
    val locale = new java.util.Locale("en", "US", "USD")
    val currencyFormat = java.text.NumberFormat.getCurrencyInstance(locale)
    
	case class User (customerID: Long, fullName: String, emails: Set[String], phoneNumbers: Set[String])
	case class Transaction (customerID: Long, amount: BigDecimal, date: String)
	// TODO: Use a Date calss instead of String for Date

	val doNotCallRDD = sc.textFile("donotcall-test.txt");
  // broadcast variable will improve performance by delivering the data once to all the nodes instead of having to serialize it with each task
  val doNotCallBC = sc.broadcast(doNotCallRDD.collect.toSet)

  
	val userLines = sc.textFile("users-test.txt")
	
	val userRegex = """^\d{1,18};[^;]*;[^;]*;[\d \,\(\)-]+""".r // validates input for further processing and filters out users without phone numbers
	val validUserLines = userLines.filter(line => userRegex.pattern.matcher(line).matches)
	
	val validUsers = validUserLines.map(_.split(";")).map(u => (u(0).toLong, User(u(0).toLong, u(1), u(2).split(",").toSet, u(3).split(",").toSet)))
	// TODO: Array to list
	// TODO: remove emails - not relevant
	// TODO: assumption is that when user have several phone numbers and only some of them are in donotcalllist, user may be contacted using other
	// TODO: diff doNotCallList assumes there are no repetitive values in phone numbers.

	val usersWithFilterdPhones = validUsers.map{ case(id, u) => (id, User(u.customerID, u.fullName, u.emails, u.phoneNumbers -- doNotCallBC.value))}
	//val usersWithPhone = users.filter(u => u.phoneNumbers.nonEmpty)

	//val usersWithPhoneWeCanCall = users.filter{case (user, d) => doNotCallBC.value.contains(d)}
	val usersWhiteList = usersWithFilterdPhones.filter{case (id, user) => user.phoneNumbers.nonEmpty}
		
	val transactions = sc.textFile("transactions-test.txt").map(_.split(";")).map(t => (t(0).toLong, Transaction(t(0).toLong, BigDecimal(currencyFormat.parse(t(1)).toString), t(2) ) ))

	/* Filter by year */
	val transactions2015 = transactions.filter{case (id, transaction) => transaction.date.startsWith("2015")}
	// TODO: re-implement using Spark date functins, which were added in Spark 1.5. For now we will use string manipulations

	/* calculate sum */
	val transactionsTotalPerUser = transactions2015.reduceByKey{ (l,r) => Transaction(l.customerID, l.amount + r.amount, l.date )}

	/* Get results */
	val userTransactions = usersWhiteList.join(transactionsTotalPerUser)
	// TODO: inner join? Can we optimize the join?
	// TODO: does the order of a join matter (one set is bigger)
	
	val topUsers = userTransactions.takeOrdered(100)(Ordering[BigDecimal].reverse.on(ut => ut._2._2.amount))
	
	val formattedResult = topUsers.map(x => "%d, %s, %s, %s".format(x._1, x._2._1.fullName, x._2._1.phoneNumbers.mkString("[", ", ", "]"), x._2._2.amount.toString))
	
	scala.tools.nsc.io.File("results").writeAll(formattedResult.mkString("\n"))
	
  }
}

/*
val regex4 = """^\d{1,10};[^;]*;[^;]*;[\d \,\(\)-]+""".r
val regex = "a.c".r
val tokens = List("abc", "axc", "abd", "azc")
tokens filter (x => regex.pattern.matcher(x).matches)


*/

