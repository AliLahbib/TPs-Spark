from pyspark import SparkContext,SparkConf

if __name__ == "__main__":
    conf= SparkConf().setAppName("Word counter - python").set("spark.hadoop.yarn.ressourcemanager.address","10.0.2.15:8032")
    sc= SparkContext(conf=conf)
    # 1. Créer un RDD avec 1000 numéros (de 0 à 999)
    rdd = sc.parallelize(range(1000))

    # 2. Prendre un échantillon SANS remise (False)
    # Chaque élément a 10% (0.1) de chance d'être pris.
    echantillon = rdd.sample(False, 0.1)

    # 3. Afficher le résultat
    # Vous verrez une liste de numéros aléatoires
    print(echantillon.collect())

    # 4. Afficher la taille
    # Le résultat sera PROCHE de 100 (10% de 1000),
    # par exemple 95, 103, 110...
    print("Taille de l'échantillon :", echantillon.count())
