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
	// TODO: Use a Date calss instead of String for Date
	
	Thoughts: 
	IMPORTANT: Thoughts have to be verified on practice
	Having 500K rows in transactions (maybe half of which are 2015) 
	and 25K users (20K of which will have phone numbers)
	=> we want to do filter sources and do calculations on nodes first, do join the latest
	Getting top 100 transactions first and join with users later would not work because users may not have valid contact phone numbers. So, we have to join full set of data first
	Although we trust data structure, string values may contain special symbols, which may break our logic. Since this is a test, we will keep a balance towards simplicity
	Broadcast improves performance by delivering the data once to all the nodes
	
  // we could add more 
  // ex: strip everything from phone numbers except numbers	
*/

object Advanced {
  def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("Spark Test App")
    val sc = new SparkContext(conf)

  	// DEFINITIONS
  	
    // parsing users definitions
  	case class User (customerID: String, fullName: String, phoneNumbers: Set[String])
  	
  	// load to RDD as User objects, taking care of possible ';' coming from special symbols in user names
    def parseUser(line: String): User = {
      val separator = ';'
      val nameIndex = line.indexOf(separator)
      val phoneIndex = line.lastIndexOf(separator)
      val emailIndex = line.lastIndexOf(separator, phoneIndex - 1)
      val customerID = line.substring(0, nameIndex)
      val name = line.substring(nameIndex + 1, emailIndex)
      val phones = line.substring(phoneIndex + 1)
      val phoneSet = if (phones.nonEmpty) phones.split(',').toSet else Set[String]()
      User(customerID, name, phoneSet)
    }
    
    // parsing transactions definitions
    case class TimedTransaction (val customerID: String, val amount: BigDecimal, calendar: java.util.Calendar)
    
	  val locale = new java.util.Locale("en", "US", "USD")
    val currencyFormat = java.text.NumberFormat.getCurrencyInstance(locale)
    
    def parseTransaction(line: String): TimedTransaction = {
  	  val tokens = line.split(';')
  	  // amount
  	  val amount = BigDecimal(currencyFormat.parse(tokens(1)).toString)
  	  // date
  	  val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd");
  	  val calendar = java.util.Calendar.getInstance()
  	  calendar.setTime(sdf.parse(tokens(2)))
  	  // construct a transaction object
      TimedTransaction(tokens(0), amount, calendar)
    }
  	
    // helper classes definitions
    case class Transaction (val customerID: String, val amount: BigDecimal)
  	case class UserTransaction (customerID: String, fullName: String, phoneNumbers: Set[String], val amount: BigDecimal)

  	
  	// THE ACTUAL WORK
  	
  	val doNotCallRDD = sc.textFile("donotcall.txt");
    val doNotCallBC = sc.broadcast(doNotCallRDD.collect.toSet)

  	val allUsers = sc.textFile("users.txt").map(x => parseUser(x))
  	val allUsersLessPhones = allUsers.map(u => u.copy(phoneNumbers=(u.phoneNumbers -- doNotCallBC.value)))
  	val usersWeCanCall = allUsersLessPhones.filter(u => u.phoneNumbers.nonEmpty).map(u => (u.customerID, u))
  		
  	val allTransactions = sc.textFile("transactions.txt").map(x => parseTransaction(x))
    val transactions2015 = allTransactions.filter(t => t.calendar.get(java.util.Calendar.YEAR) == 2015)
    val transactionTupes = transactions2015.map(t => (t.customerID, Transaction(t.customerID, t.amount)))
    val transactionTotals = transactionTupes.reduceByKey((l, r) => Transaction(l.customerID, l.amount + r.amount))
    
  	val userTransactions = usersWeCanCall.join(transactionTotals).
  	  map(ut => UserTransaction(ut._2._1.customerID, ut._2._1.fullName, ut._2._1.phoneNumbers, ut._2._2.amount))
  	  
  	val topUsers = userTransactions.takeOrdered(100)(Ordering[BigDecimal].reverse.on(ut => ut.amount))
  	
  	val formattedResult = topUsers.map(
  	  ut => "%s, %s, %s, %s".format(
        ut.customerID, 
        ut.fullName, 
        ut.phoneNumbers.mkString("\"", ", ", "\""), 
        currencyFormat.format(ut.amount) )) // ut.amount.toString
  	scala.tools.nsc.io.File("results-advanced.csv").writeAll(formattedResult.mkString("\n"))
  	
  }
}
