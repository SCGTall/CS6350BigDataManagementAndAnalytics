Upload soc-LiveJournal1Adj.txt into input1  and userdata.txt into input2

    Q1:
1. under /Q1 directory
2. input: hadoop jar MutualFriends.jar /input1 /output1
3. output : /output1
4. You can also use MutualFriendsUnfiltered.jar to get all mutual friends. For example:
	hadoop jar MutualFriendsUnfiltered.jar /input1 /output1_2

    Q2:
1. under /Q2 directory
2. input : hadoop jar MaxCommonFriends.jar /output1 /output2
3. output : /output2
4. You can also get the maximum number which is not limited by Q1. For example:
	hadoop jar MaxCommonFriends.jar /output1_2 /output2_2

    Q3:
1. under /Q3 directory
2. input : hadoop jar InMemoryMapperJoin.jar 6222 19272 /input1 /input2 /output3
3. output : /output3
4. The example output in the document has contradiction. Below is my format:
	For I did not use a reducer here, for each line: <userA>,<userB>\t<list>.
	For each item in list, I also list both first name and last name: [fName lName:dob].
5. demo: 0, 38

    Q4
1. under /Q4 directory
2. input : hadoop jar InMemoryReduceJoin.jar /input1 /input2 /output4
3. output : /output4
4. demo: need to consider the month and day for age.

    Q5
1. under /Q5 directory
2. input : hadoop jar InvertedIndex.jar /input2 /output5
3. output : /output5
4. Split each line to words by "," or " ". Also remove extra parenthesis if necessary.

All source codes are included in /src directory. I also add configuration files for maven plugin.

Good luck.