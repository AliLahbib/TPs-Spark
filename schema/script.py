from pyspark.sql.types import StructField, StructType, StringType, IntegerType
from pyspark.sql import SparkSession,Row
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import col, struct, when, lit
import json

if __name__ == "__main__":
    spark = SparkSession.builder.appName("DataFrame").getOrCreate()
    data = [
        ("1201", ("Foulen", "BenAli"), 25, 100),
        ("1202", ("Filtena", "Trabelsi"), 24, 200),
        ("1203", ("Flen", "Sassi"), 40, 150),
        ("1204", ("Foulena", "Jlassi"), 23, 80),
        ("1201", ("Filten", "Bouhlel"), 25, 50),
    ]
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField(
                "name",
                StructType(
                    [
                        StructField("firstname", StringType(), True),
                        StructField("lastname", StringType(), True),
                    ]
                ),
                True,
            ),
            StructField("age", IntegerType(), True),
        ]
    )

    with open("schema.json") as f:
        schemaFromJson = StructType.fromJson(json.load(f))

    df = spark.createDataFrame(data, schemaFromJson)
    df.printSchema()
    df.show(truncate=True)

    updated = df.withColumn(
        "OtherInfos",
        struct(
            col("name.firstname").alias("firstname"),
            col("id").alias("identifier"),
            col("salary").alias("salary"),
            lit(0).alias("status"),
            when(col("salary").cast(IntegerType()) < 100, "Low")
            .when(col("salary").cast(IntegerType()) < 200, "Medium")
            .otherwise("High")
            .alias("Salary_grade"),
        ),
    ).drop("id", "salary")
    updated.printSchema()
    updated.show(truncate=False)
    
    updated.registerTempTable("people")
    
    spark.sql("select name,age from people").show()
    spark.sql("select name.*,age from people").show()
    
    
    
    def convertCase(str):
        resStr=""
        arr= str.split(" ")
        for x in arr:
            resStr= resStr + x[0:1].upper()+ x[1:len(x)]+ " "
        return resStr
    

 
        
