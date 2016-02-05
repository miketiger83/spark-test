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
	
	val NumberFormat currencyFormat = new java.text.NumberFormat(new java.util.Locale("en", "US", "USD")).getCurrencyInstance();

	case class User (customerID: Long, fullName: String, emails: List[String], phoneNumbers: List[String])
	case class Transaction (customerID: Long, amount: BigDecimal, date: String)
	// TODO: Use a Date calss instead of String for Date

	val doNotCallList = sc.textFile("donotcall.txt");

	val users = sc.textFile("users.txt").map(_.split(";")).map(u => (u(0), User(u(0), u(1), map(u(2).split(",")), map(u(3).split(",")) diff doNotCallList )));
	// TODO: assumption is that when user have several phone numbers and only some of them are in donotcalllist, user may be contacted using other
	// TODO: diff doNotCallList assumes there are no repetitive values in phone numbers.

	val usersWithPhone = users.filter(u => u.phoneNumbers.nonEmpty)

	val transactions = sc.textFile("transactions.txt").map(_.split(";")).map(t => (t(0), Transaction(t(0), currencyFormat.parse(t(1)), t(2) )
	));

	/* Filter by year */
	val transactions2015 = transactions.filter(t => t.date.startsWith("2015"))
	// TODO: re-implement using Spark date functins, which were added in Spark 1.5. For now we will use string manipulations

	/* calculate sum */
	val userTransactions = transactions2015.reduceByKey(_.amount + _.amount)

	/* Get results */
	usersWithPhone.join(userTransactions).take(1000)
	// TODO: inner join? Can we optimize the join?
	// TODO: does the order of a join matter (one set is bigger)
  }
}
