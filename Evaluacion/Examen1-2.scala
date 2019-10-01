import org.apache.spark.sql.SparkSession
//1
val spark = SparkSession.builder().getOrCreate()
//2
val df = spark.read.option("header", "true").option("inferSchema","true")csv("Netflix_2011_2016.csv")
//3
printf("El nombre de las columnas es:")
df.columns
//4
printf("El esquima")
df.printSchema()
//5
printf("Las primeras 5 columnas")
df.show(5)
//6
printf("Describe para aprender del DataFrame")
df.describe()
//7 Crear el data Frame HVRatio
val df2 = df.withColumn("HV Ratio", df("High")/df("Volume"))
df2.select("HV Ratio").show()
//8
printf("El dia del pico mas alto de la columna Price, no existe la columna")

//9
print("El archivo completo es de los precio de las acciones en la bolsa, por lo tanto el valor de Close es el el valor al cual se cerro el precio de ese dia.")
//10
printf("El maximo del volumen es:")
df.select(max("Volume")).show()
printf("El minimo de volumen es:")
df.select(min("Volume")).show()
//11
//a)
printf("Dias que la columna close es inferior a  600")
df.filter($"Close" < 600).count()
//b)
printf("Porcentaje de tiempo columna high fue mayor que 500")
val total1 = (df.filter($"High" > 500).count())* 1.0
val total2 = total1 / (df.select("High").count())
printf(s"El porcentaje es $total2" )
//c)
printf("Correlacion entre High y Volumne")
df.select(corr("High", "Volume")).show()
//d)
printf("Maximo de Higth por anio")
val df2=df.withColumn("Year",year(df("Date")))
val dfmax=df2.groupBy("Year").max()
dfmax.select($"Year",$"max(High)").show()
//e)
printf("Promedio de la columna close por mes")
val dfmonth=df.withColumn("Month",month(df("Date")))
val dfmean=dfmonth.select($"Month",$"Close").groupBy("Month").mean()
dfmean.orderBy($"Month").show()