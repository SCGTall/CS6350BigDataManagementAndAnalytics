# coding: utf-8

import os
import shutil
import re

from pyspark import SparkContext

sc = SparkContext.getOrCreate()
friendsDir = sc.textFile("soc-LiveJournal1Adj.txt").persist()
userdataDir = sc.textFile("userdata.txt").persist()
reviewDir = sc.textFile("review.csv").persist()
businessDir = sc.textFile("business.csv").persist()



# Question 7
def mapFunc7(userdata):
    inp = userdata.split("\n")
    outp = []
    for line in inp:
        wordList = re.split(r'[,｜ ]', line)
        lineNum = int(wordList[0])
        for word in wordList[1:]:
            relaxed = word.strip().lstrip("(").rstrip(")")
            if (len(relaxed) > 0):
                tuple = (lineNum, relaxed)
                outp.append(tuple)
    return outp

GIVEN_WORD = "Paula"
map7 = userdataDir.flatMap(mapFunc7)
reduce7 = map7.sortByKey().map(lambda x: (x[1], x[0])).groupByKey().map(lambda x: (x[0], list(x[1])))
output7 = reduce7.filter(lambda x: x[0] == GIVEN_WORD).map(lambda x: x[0] + ", " + str(x[1]))

if os.path.exists("Q7"):
    shutil.rmtree("Q7")
output7.coalesce(1, True).saveAsTextFile("Q7")
