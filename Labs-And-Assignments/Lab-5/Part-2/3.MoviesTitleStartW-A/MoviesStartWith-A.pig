register '/usr/lib/pig/piggybank.jar';

recordLists = LOAD '/home/cloudera/Desktop/Lab-5/Part-2/3.MoviesTitleStartW-A/movies.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage() AS (movie_Id:int, title:chararray, genres:chararray);
filterred = FILTER recordLists BY STARTSWITH (title,'A') OR  STARTSWITH (title,'a'); 
generated_genres = FOREACH  filterred  GENERATE genres ;
tokenized = FOREACH generated_genres GENERATE TOKENIZE(genres,'|') AS wordBag;
flattened = FOREACH tokenized GENERATE flatten($0) as word;
grouped = GROUP flattened by word;
counted = FOREACH grouped GENERATE group, COUNT(flattened);
ordered = ORDER counted BY group;
STORE ordered INTO 'output2.3' USING PigStorage('*');
