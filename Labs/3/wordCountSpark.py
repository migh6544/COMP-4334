# This is word count in Spark using map/filter

sc = spark.sparkContext

# You need to click on the databricks button
# Then drag wc.txt onto the import
# And go ahead and make the table that goes with it
#   so it will be loaded into the dbfs and available
#   fo all the worker nodes to read
txtFile = "dbfs:///FileStore/tables/wc.txt"

# load in the file
file = sc.textFile(txtFile)
#file = sc.parallelize(['the quick brown fox', 'jumps over the lazy dog'])
print(file.collect())

#file = sc.parallelize(['the quick brown fox', 'jumps over the lazy dog'])

words = file.flatMap(lambda line: line.split(" "))

print(words.collect())

# turn each word into a tuple (word, 1) with a map
# I can use either tuples (can't be modify) or lists
wordTuples = words.map(lambda word: (word, 1))
#wordTuples = words.map(lambda word: [word, 1])
print(wordTuples.collect())

# reduce by the word (key) and add the values
# note that we could also use an actual function in place of the lambda
counts = wordTuples.reduceByKey(lambda a, b: a + b)
print(counts.collect())
