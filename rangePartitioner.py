from pyspark.sql import SparkSession
from pyspark import SparkContext,SparkConf

conf= SparkConf().setAppName("")
sc= SparkContext(conf=conf)
spark = SparkSession.builder.appName('').getOrCreate()

rdd = sc.parallelize([
    (8, 'a'),
    (96, 'b'),
    (240, 'c'),
    (400, 'd'),
    (800, 'e')
])
print("RDD initial (probablement 1 seule partition par défaut) :")
print(rdd.glom().collect())
print("-" * 40)
print("\nTEST 1: partitionBy(4) -- (Utilise HashPartitioner)")
rdd_hash = rdd.repartition(4)

# .glom() nous montre le contenu de chaque partition
result_hash = rdd_hash.glom().collect()
print("Résultat (Déséquilibré) :")
print(result_hash)
print("-> Toutes les données sont dans la Partition 0. Échec.")
print("-" * 40)
print("\nTEST 2: partitionBy(4, RangePartitioner)")

# Note: RangePartitioner a besoin de connaître les bornes,
# ce qui peut déclencher un petit "job" d'échantillonnage.
rdd_range = rdd.part(4)

result_range = rdd_range.glom().collect()
print("Résultat (Équilibré) :")
print(result_range)
print("-> Les données sont bien réparties par plages de clés.")
print("-" * 40)
sc.stop()