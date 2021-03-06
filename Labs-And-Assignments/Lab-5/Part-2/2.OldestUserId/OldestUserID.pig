recordLists = LOAD '/home/cloudera/Desktop/Lab-5/Part-2/2.OldestUserId/users.txt' USING PigStorage ('|') AS (userId:int, age:int, gender:chararray, occupation:chararray, zipCode:long);

filteredList = FILTER recordLists BY gender == 'M' AND occupation == 'lawyer';
order_By_age = ORDER filteredList BY age DESC;
limitTo1 = LIMIT order_By_age 1;
generateUserID = FOREACH limitTo1 GENERATE userId ;
STORE generateUserID INTO 'output2.2' USING PigStorage('*'); 

