import sys
from pyspark import SparkContext,SparkConf


def parse_line(line):
    parts = line.split(',')
    timestamp = parts[4]
    return (int(parts[0]), int(parts[1]), int(parts[2]), float(parts[3]), timestamp)

if __name__ == "__main__":
    conf= SparkConf().setAppName("Word counter - python").set("spark.hadoop.yarn.ressourcemanager.address","10.0.2.15:8032")
    sc= SparkContext(conf=conf)
    
    p="""1,1001,2001,50.5,2024-01-01 10:30:00
    2,1002,2002,75.0,2024-01-01 11:00:00
    3,1001,2003,60.0,2024-01-01 11:30:00
    4,1003,2003,80.0,2024-01-01 12:00:00
    5,1001,2001,80.0,2024-01-01 12:30:00
    6,1002,2003,80.0,2024-01-01 13:00:00"""
    
    lines_rdd = sc.parallelize(p.strip().split('\n'))
    lines_rdd_parsed=lines_rdd.map(parse_line)
    lines_rdd_parsed.persist()
    print("*"*100)
    for line in lines_rdd_parsed.collect()[:5]:
        print(line)
    print("*"*100)
    print("len of rdd",lines_rdd_parsed.count())
    print("*"*100)
    tot=lines_rdd_parsed.map(lambda x: x[3]).sum()
    print("somme des montant ",tot)
    print("*"*100)
    mean=lines_rdd_parsed.map(lambda x: x[3]).mean()

    print("moyenne des montant ",mean)
    print("*"*100)
    
    plus_3=lines_rdd_parsed.map(lambda t: (t[1], 1)) \
                                   .reduceByKey(lambda a, b: a + b) \
                                   .sortBy(lambda x: x[1], ascending=False) \
                                   .take(3)
    print("les plus vendu ",plus_3)
    print("*"*100)
 

