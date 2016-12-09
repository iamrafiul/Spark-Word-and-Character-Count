# @Author: mdrhri-6
# @Date:   2016-12-09T05:55:12+01:00
# @Last modified by:   mdrhri-6
# @Last modified time: 2016-12-09T15:58:09+01:00



import sys
import csv

from pyspark import SparkContext, SparkConf

output = list()

# create Spark context with Spark configuration
conf = SparkConf().setAppName("Spark Word Count")
sc = SparkContext(conf=conf)

# read text file and slpit each word
tokens = sc.textFile("hdfs://localhost/user/root/textfile.txt").flatMap(lambda line: line.split(" "))

# count the occurance of each word
word_counts = tokens.map(lambda word: (word, 1)).reduceByKey(lambda v1, v2: v1 + v2)

collect_words = word_counts.collect()
output.append("Word List: {0}".format(collect_words))
output.append("Word count: {0}".format(len(collect_words)))

# filter words which occured less than 2 times.
filtered = word_counts.filter(lambda each: each[1] >= 2)

# count characters
char_counts = filtered.flatMap(lambda each: each[0]).map(lambda char: char).map(lambda c: (c, 1)).reduceByKey(lambda v1, v2: v1 + v2)

char_list = char_counts.collect()

output.append("Character List: {0}".format(char_list))
output.append("Character Count: {0}".format(len(char_list)))

# write output in a text file
f = open("output.txt", "w")
for each in output:
    f.write(each + "\n")
f.close()

# write word count in a csv file
fp = open("output.csv", "w")
writer = csv.writer(fp, dialect="excel")
writer.writerows(collect_words)
fp.close()

# write character cound in a csv file
fp = open("char_output.csv", "w")
writer = csv.writer(fp, dialect="excel")
writer.writerows(char_list)
fp.close()
