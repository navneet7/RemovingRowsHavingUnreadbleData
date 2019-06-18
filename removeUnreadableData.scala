
import org.apache.log4j._

import spark.implicits._

object removeUnreadableData extends App{
  
  //Setting logger for Level = ERROR
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val filePath = yourBasePath+"/sample_file.csv"
  
  //Reading csv as a dataframe
  val df = spark.read.option("header","true").option("inferSchema", "true").csv(filePath)
  //Printing the count
  println(df.count())
  //Using regular expression to remove rows that are having unreadable data
  val df1 = df.filter($"Purchase order number" rlike "^([\\x00-\\x7F])+$")
  //Printing the count after removing junk rows
  println(df1.count)
  //Getting all junk rows as a dataframe
  val df_error = df.except(df1)
  //Showing all junk row
  df_error.show
}
