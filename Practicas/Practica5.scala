import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

val df = spark.read.option("header", "true").option("inferSchema","true")csv("Sales.csv")

//1
df.groupBy("Company").mean().show()

//2
df.groupBy("Company").count().show()

//3
df.groupBy("Company").max().show()

//4
df.groupBy("Company").min().show()

//5
df.groupBy("Company").sum().show()

//6

//7

//8

//9

//10

//11

//12

//13

//14

//15

//16

//17

//18

//19

//20