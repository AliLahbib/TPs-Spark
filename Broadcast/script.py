import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = (
        SparkConf()
        .setAppName("Word counter - python")
        .set("spark.hadoop.yarn.ressourcemanager.address", "10.0.2.15:8032")
    )
    sc = SparkContext(conf=conf)

    data = [
        ("med amine", "fradi", "TUN", "SO"),
        ("amine", "lamti", "TUN", "BA"),
        ("ali", "lahbib", "TUN", "BZ"),
        ("Tarek", "ben mena", "TUN", "AR"),
    ]

    states = {"SO": "Sousse", "BA": "Ben Arous", "BZ": "Bizerte", "AR": "Ariana"}
    rdd= sc.parallelize(data)

    statesBrod= sc.broadcast(states)
    
    rddReplacedstate=rdd.map(lambda pers: 
            [pers[0],pers[1],pers[2],statesBrod.value.get(pers[3])]
        )
    result=rddReplacedstate.collect()
    for personne in result :
        print(personne)
    
    