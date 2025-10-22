

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('BroadCast Variables').getOrCreate()

states = {"TN":"Tunis", "BZ":"Bizerte", "KF":"Kef"}

broadcastStates = spark.sparkContext.broadcast(states)

data = [("Foulen","Ben Foulen","TUN","TN"),
        ("Filten","Ben Filten","TUN","KF"),
        ("Filtena","Bint Flen","TUN","BZ"),
        ("Flen","Ben Mohamed","TUN","TN")
       ]

columns = ["firstname","lastname","country","state"]

df = spark.createDataFrame(data = data, schema = columns)
df.printSchema()
df.show(truncate=False)

def state_convert(code):
   return broadcastStates.value[code]

#Affichage sous forme Data Frame
result = df.rdd.map(lambda x: (x[0],x[1],x[2],state_convert(x[3]))).toDF()
result.show(truncate=False)

#Conversion DF ==> RDD et Affichage sous forme RDD
result = result.rdd.map(tuple).collect()
print(result)