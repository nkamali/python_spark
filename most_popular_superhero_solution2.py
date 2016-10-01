from pyspark import SparkConf, SparkContext
import inspect

# DOES NOT USE BROADCAST VARIABLES
conf = SparkConf().setMaster("local").setAppName("PopularSuperHero")
sc = SparkContext(conf = conf)

def countCoOccurrences(line):
  elements = line.split()
  return (int(elements[0]), int(len(elements) - 1))

def parseName(line):
  fields = line.split('"')
  return (int(fields[0]), fields[1].strip().replace('"', ''))

names = sc.textFile('Marvel-Names.txt')
namesRdd = names.map(parseName)

lines = sc.textFile('Marvel-Graph.txt')
heroes = lines.map(countCoOccurrences)

cooccurrences = heroes.reduceByKey(lambda x,y: x + y)
flipped = cooccurrences.map(lambda x: (x[1], x[0]))
mostPopular = flipped.max() # finds the max key value

mostPopularName = namesRdd.lookup(mostPopular[1])[0]
count = mostPopular[0]

print(mostPopularName + " is the most popular superhero, with " + \
  str(count) + " co-appearances.")
