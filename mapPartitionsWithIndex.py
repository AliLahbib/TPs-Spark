from pyspark import SparkContext,SparkConf

if __name__ == "__main__":
    conf= SparkConf().setAppName("Word counter - python").set("spark.hadoop.yarn.ressourcemanager.address","10.0.2.15:8032")
    sc= SparkContext(conf=conf)
    # 1. Créer un RDD avec 2 partitions
    rdd = sc.parallelize(["un", "deux", "trois", "quatre"], 2)  # Le "2" force 2 partitions


    # 2. Définir notre fonction qui s'applique à une partition ENTIÈRE (avec index)
    def fonction_partition(index, iterateur):

        resultats = []
        for mot in iterateur:
            resultats.append(f"PREP-{mot} (partition={index})")

        return iter(resultats)  # On retourne un itérateur


    # 3. Appliquer la fonction en recevant l'index de partition
    rdd_transforme = rdd.mapPartitionsWithIndex(lambda idx, iterateur: fonction_partition(idx, iterateur))

    # 4. Lancer le calcul
    print("\nRésultat de mapPartitionsWithIndex :")
    for mot in rdd_transforme.collect():
        print(mot)

    # Fermer proprement le contexte Spark
    sc.stop()
