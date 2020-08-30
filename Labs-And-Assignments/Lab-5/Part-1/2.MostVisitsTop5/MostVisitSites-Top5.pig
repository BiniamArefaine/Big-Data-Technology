recordUsers = LOAD '/home/cloudera/Desktop/Lab-5/Part-1/2.MostVisitsTop5/users.csv' USING PigStorage (',') AS (name: chararray, age: int);
filteredUsers = FILTER recordUsers BY age >= 18 and age <= 25;
visitedPages = LOAD '/home/cloudera/Desktop/Lab-5/Part-1/2.MostVisitsTop5/pages.csv' USING PigStorage (',') AS (user: chararray, site: chararray);
userURL = JOIN filteredUsers BY name, visitedPages BY user;
group_User = GROUP userURL BY site;
count_User = FOREACH group_User GENERATE group, COUNT(userURL) AS count;
order_User = ORDER count_User BY count DESC;
LimitTo5 = LIMIT order_User 5;
STORE LimitTo5 INTO 'output2' USING PigStorage('\t');

