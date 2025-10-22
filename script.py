from pyspark import SparkConf, SparkContext

# Configuration Spark
conf = SparkConf().setAppName("WordCountWithShuffle").setMaster("local[*]")
sc = SparkContext(conf=conf)

# Lecture du fichier texte
text_file = sc.textFile("data/text.txt")

# Word Count de base
word_counts = (
    text_file
    .flatMap(lambda line: line.split())
    .map(lambda word: (word.lower(), 1))
    .reduceByKey(lambda a, b: a + b)   # ⚙️ Shuffle #1 (reduceByKey)
)

# 🔥 Étape supplémentaire pour forcer d'autres shuffles
# 1️⃣ Trier les mots par fréquence décroissante
sorted_counts = (
    word_counts
    .map(lambda x: (x[1], x[0]))       # inverser (mot, count) → (count, mot)
    .sortByKey(ascending=False)        # ⚙️ Shuffle #2 (sortByKey)
)

# 2️⃣ Joindre avec un autre RDD (autre shuffle)
# Créons un petit RDD de catégories fictives
categories = sc.parallelize([
    ("spark", "framework"),
    ("world", "general"),
    ("hello", "greeting"),
])

joined = (
    sorted_counts
    .map(lambda x: (x[1], x[0]))       # (mot, count)
    .join(categories)                  # ⚙️ Shuffle #3 (join)
)

# Sauvegarde des résultats
output_path = "output/wordcount_shuffle"
joined.saveAsTextFile(output_path)

print(f"\n✅ Résultat sauvegardé dans : {output_path}")
print("📊 Ouvre ton navigateur sur : http://localhost:4040 pour voir les Jobs, Stages et Shuffles\n")

# Maintenir l'UI Spark ouverte
input("➡️  Appuie sur [Entrée] pour fermer Spark et quitter...")

sc.stop()
