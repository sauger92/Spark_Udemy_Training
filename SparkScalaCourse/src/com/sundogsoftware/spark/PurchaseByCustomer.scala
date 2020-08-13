package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.min

object PurchaseByCustomer {
    def parseLine(line:String)= {
    val fields = line.split(",")
    val customerId = fields(0).toInt
    val amountSpent = fields(2).toFloat
    (customerId, amountSpent)
  }
    
        /** Our main function where the action happens */
  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "PurchaseByCustomer")
    
    // Read each line of input data
    val lines = sc.textFile("../customer-orders.csv")
    
    // Convert to (stationID, entryType, temperature) tuples
    val parsedLines = lines.map(parseLine)
    
    val mntByCustomer = parsedLines.reduceByKey( (x,y) => x+y)
    

    
// Flip (word, count) tuples to (count, word) and then sort by key (the counts)
    val mntByCustomerSorted = mntByCustomer.map( x => (x._2, x._1) ).sortByKey()
    
    // Print the results, flipping the (count, word) results to word: count as we go.
    for (result <- mntByCustomerSorted) {
      val mnt = result._1
      val customer = result._2
      println(s"$customer spent: $mnt")
    }
  }

    
    
    
}