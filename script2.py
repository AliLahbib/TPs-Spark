import sys
from pyspark import SparkContext,SparkConf

if __name__ == "__main__":
    conf= SparkConf().setAppName("Word counter - python").set("spark.hadoop.yarn.ressourcemanager.address","10.0.2.15:8032")
    sc= SparkContext(conf=conf)
    lines=sc.textFile("data/")
    print("recieved")
    words=lines.flatMap(lambda line : line.split(" "))
    words_with_1=words.map(lambda word: (word, 1))
    wordCounts= words_with_1.reduceByKey(lambda a,b:a+b)
   
    result= wordCounts.collect()
    for(word,count) in result :
        print(word,count)
   