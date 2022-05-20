
import com.snowflake.snowpark._
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark.types._


object Main {
  def main(args: Array[String]): Unit = {
    // Replace the <placeholders> below.
    val configs = Map (
      "URL" -> "https://<account_identifier>.snowflakecomputing.com:443",
      "USER" -> "<USER_NAME>,
      "PASSWORD" -> "<PASSWORD>",
      "ROLE" -> "<ROLE>",
      "WAREHOUSE" -> "<WAREHOUSE>",
      "DB" -> "<DB>",
      "SCHEMA" -> "<SCHEMA>"
    )
    // Creating session 
    val session = Session.builder.configs(configs).create
    
    // Create DF from data present in table "sample_product_data"
    val dfTable = session.table("sample_product_data")
    // To print out the first 10 rows
    dfTable.show()

    // Create a DataFrame from a SQL query
    val dfSql = session.sql("SELECT * from sample_product_data where ID > 5")
    dfSql.show()
    
    //Retrieving Column Definitions
    val dfTableSchema = dfTable.schema
    println(s"Schema of 'sample_product_data': ${dfTableSchema}")

    // Return number of rows
    val count = dfTable.count();
    println(s"Total number of rows: ${count}")

    // Filtering columns
    val dfFilter = dfTable.filter(col("ID") < 5)
    dfFilter.show()

    // Selecting specific columns & renaming columns
    val dfSelect = dfTable.select(col("ID").as("NO"), col("CATEGORY_ID"), col("NAME"))
    dfSelect.show() 

    // Sorting Data and limiting number of rows
    val dfSort = dfTable.sort(col("ID").desc).limit(4)
    dfSort.show()

    // Updating rows
    val updateResult = dfTable.update(Map("3rd" -> lit(1)), col("Id") === 4)
    println("Number of rows updated: ", updateResult.rowsUpdated)

    // Deleting rows
    val deleteResult = dfTable.delete(dfTable("ID" )=== 8)
    println(s"Number of rows deleted: ${deleteResult.rowsDeleted}")

    // Merging rows into table

    val dfTarget = session.table("Products")
    val dfSource = session.table("Products_Info")
    val mergeResult = dfTarget.merge(dfSource, dfTarget("ProductID") === dfSource("ProductID"))
                              .whenNotMatched.insert(Seq(dfSource("ProductID"),dfSource("Product_Name"), dfSource("Cost")))
                              .whenMatched.update(Map("Product_Name" -> dfSource("Product_Name"), "Cost" -> dfSource("Cost")))
                              .collect()
                      
    println(s"Merge result: ${mergeResult}")

    // Joining DataFrames
    val dfLeftTable = session.table("sample_a")
    val dfRightTable = session.table("sample_b")

    val dfjoined = dfLeftTable.join(dfRightTable, dfLeftTable.col("ID_A") === dfRightTable.col("ID_A"))
    dfjoined.show()

    // Saving joined data into a table
    dfjoined.write.mode(SaveMode.Append).saveAsTable("JOIN_TABLE")

    // Creating UDF 
    val dfRange = session.range(1, 10)
    session.udf.registerPermanent("demoDoubleUdfNew", (x: Int) => x + x, "mystage")
    val dfWithDoubleNum = dfRange.withColumn("doubleNum", callUDF("demoDoubleUdfNew", col("ID") ))
    dfWithDoubleNum.show()

    //close session
    session.close()
    
  }
}