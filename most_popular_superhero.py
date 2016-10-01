from pyspark import SparkConf, SparkContext
import inspect

# USES BROADCAST VARIABLES

def loadSuperheroes():
  heroNames = {}
  with open("Marvel-Names.txt", encoding = "ISO-8859-1", errors='ignore') as f:
    for line in f:
      fields = line.split(' ', 1)
      heroNames[int(fields[0])] = fields[1].strip().replace('"', '')
  return heroNames

def countCoOccurrences(line):
  elements = line.split()
  return (int(elements[0]), int(len(elements) - 1))

conf = SparkConf().setMaster("local").setAppName("PopularSuperHero")
sc = SparkContext(conf = conf)

# loads the super hero hash into a broadcast variable
nameDict = sc.broadcast(loadSuperheroes())

lines = sc.textFile('Marvel-Graph.txt')

heroes = lines.map(countCoOccurrences)

cooccurrences = heroes.reduceByKey(lambda x,y: x + y)
flipped = cooccurrences.map(lambda x: (x[1], x[0]))
mostPopular = flipped.max() # finds the max key value

count = mostPopular[0]
mostPopularName = nameDict.value[mostPopular[1]]

print(mostPopularName + " is the most popular superhero, with " + \
  str(count) + " co-appearances.")
