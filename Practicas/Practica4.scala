//Operaciones con el df
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

val df = spark.read.option("header", "true").option("inferSchema","true")csv("CitiGroup2006_2008")

//1
df.show()
//2
df.filter($"Close" < 480 && $"High" < 480).show()
//3
df.select(corr("High", "Low")).show()
//4
df.columns
//5
df.count
//6
df.select(corr("High", "Low")).show()
//7
df.select(sum("High")).show()
//8
df.select(min("High")).show()
//9
df.select(max("High")).show()
//10

//11
df.select(mean("High")).show()
//12

//13

//14

//15

//16

//17

//18

//19

//20