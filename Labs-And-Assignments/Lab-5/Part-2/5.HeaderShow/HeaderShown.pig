register '/usr/lib/pig/piggybank.jar';

first_records = load '/home/cloudera/Desktop/Lab-5/Part-2/4.HighestRated-20/movies.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage() AS (movie_Id:int,title:chararray,genres:chararray);

filtered_genres = FILTER first_records BY (genres matches '.*(Adventure).*'); 

second_records = load '/home/cloudera/Desktop/Lab-5/Part-2/4.HighestRated-20/rating.txt' AS (userId:int,movie_Id:int,rating:int,timestamp:chararray);

filtered_rating = FILTER second_records BY rating==5;

joined_Result = JOIN filtered_genres BY movie_Id, filtered_rating BY movie_Id;

results = FOREACH joined_Result GENERATE filtered_genres::movie_Id,  'Adventure' AS genre, filtered_rating::rating, filtered_genres::title;

distinct_Movies = DISTINCT results;

ordered = ORDER distinct_Movies BY movie_Id;

limitTo20 = LIMIT ordered 20;

header = FOREACH limitTo20 GENERATE $0 AS movie_Id, $1 AS Genres, $2 AS Ratings, $3 AS Titles;

STORE header INTO 'output2.5' USING org.apache.pig.piggybank.storage.CSVExcelStorage('\t', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER');





