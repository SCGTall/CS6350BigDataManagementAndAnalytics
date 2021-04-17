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


# Question 1
def mapFunc(friends):
    inp = friends.split("\t")
    outp = []
    if len(inp) < 2:
        return []

    userID = inp[0]
    friendIDs = set(inp[1].split(","))

    for friend in friendIDs:
        if not friend.isnumeric():
            continue
        pair_key = ",".join(sorted([userID, friend]))
        tuple = (pair_key, inp[1])
        outp.append(tuple)
    return outp


def reduceFunc(x):
    if len(x) != 2:
        return ""
    set1 = set(x[0].split(","))
    set2 = set(x[1].split(","))
    return set1.intersection(set2)


map1 = friendsDir.flatMap(mapFunc).groupByKey().map(lambda x: (x[0], list(x[1])))
reduce1 = map1.mapValues(reduceFunc).sortByKey().mapValues(lambda x: len(x))
output1 = reduce1.filter(lambda x: x[1] > 0).map(lambda x: x[0] + "\t" + str(x[1]))

if os.path.exists("Q1"):
    shutil.rmtree("Q1")

output1.coalesce(1, True).saveAsTextFile("Q1")

# Question 2
maxCount = reduce1.max(lambda x: x[1])[1]
output2 = reduce1.filter(lambda x: x[1] == maxCount).map(lambda x: x[0] + "\t" + str(x[1]))

if os.path.exists("Q2"):
    shutil.rmtree("Q2")

output2.coalesce(1, True).saveAsTextFile("Q2")


# Question 3
# "review_id"::"user_id"::"business_id"::"stars"
def reviewInfo1(reviews):
    review_1_line = reviews.split("::")
    review_1_key = review_1_line[2]
    review_1_value = (review_1_line[1], review_1_line[3])
    return (review_1_key, review_1_value)


# "business_id"::"full_address"::"categories"
def businessInfo1(biz):
    biz_1_line = biz.split("::")
    biz_1_key = biz_1_line[0]
    biz_1_value = biz_1_line[1]
    return (biz_1_key, biz_1_value)


review1 = reviewDir.map(reviewInfo1)
business1 = businessDir.map(businessInfo1).filter(lambda x: "Stanford" in x[1])

output3 = business1.leftOuterJoin(review1).distinct().map(lambda x: x[1][1][0] + "\t" + x[1][1][1])

if os.path.exists("Q3"):
    shutil.rmtree("Q3")

output3.coalesce(1, True).saveAsTextFile("Q3")


# Question 4
# "review_id"::"user_id"::"business_id"::"stars"
def reviewInfo2(reviews):
    review_2_line = reviews.split("::")
    review_2_key = review_2_line[2]
    review_2_value = (review_2_line[3], 1)
    return (review_2_key, review_2_value)


# "business_id"::"full_address"::"categories"
def businessInfo2(biz):
    biz_2_line = biz.split("::")
    biz_2_key = biz_2_line[0]
    biz_2_value = (biz_2_line[1], biz_2_line[2])
    return (biz_2_key, biz_2_value)


def wholeReview(v1, v2):
    rate = float(v1[0]) + float(v2[0])
    count = v1[1] + v2[1]
    return (rate, count)


def averageMap(rates):
    avgkey = rates[0]
    value = rates[1]
    avgvalue = str(float(value[0]) / float(value[1]))
    return (avgkey, avgvalue)


review2 = reviewDir.map(reviewInfo2)
business2 = businessDir.map(businessInfo2)
avgResult = review2.reduceByKey(wholeReview).map(averageMap)
temp = avgResult.leftOuterJoin(business2).distinct().top(10, key=lambda x: float(x[1][0]))
output4 = sc.parallelize(temp).map(lambda x: x[0] + "\t" + x[1][1][0] + "\t" + x[1][1][1] + "\t" + x[1][0])

if os.path.exists("Q4"):
    shutil.rmtree("Q4")
output4.coalesce(1, True).saveAsTextFile("Q4")


# Question 5
def categoryMap(line):
    record = line.split("::")
    if len(record) < 3:
        return

    categories = set(record[2][5:-1].split(","))
    categoryList = []
    for category in categories:
        if (len(category.strip()) > 0):
            categoryList.append((category.strip(), 1))
    return categoryList


output5 = businessDir.distinct().flatMap(categoryMap).reduceByKey(lambda a, b: a + b).map(
    lambda x: str(x[0]) + "\t" + str(x[1]))

if os.path.exists("Q5"):
    shutil.rmtree("Q5")
output5.coalesce(1, True).saveAsTextFile("Q5")

# Question 6
output6 = output5.top(10, key=lambda x: int(x.split("\t")[1]))

if os.path.exists("Q6"):
    shutil.rmtree("Q6")
output6 = sc.parallelize(output6).coalesce(1, True).saveAsTextFile("Q6")


# Question 7
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

GIVEN_WORD = "Rachel"
map7 = userdataDir.flatMap(mapFunc7)
reduce7 = map7.sortByKey().map(lambda x: (x[1], x[0])).groupByKey().map(lambda x: (x[0], list(x[1])))
output7 = reduce7.filter(lambda x: x[0] == GIVEN_WORD).map(lambda x: x[0] + ", " + str(x[1]))

if os.path.exists("Q7"):
    shutil.rmtree("Q7")
output7.coalesce(1, True).saveAsTextFile("Q7")
