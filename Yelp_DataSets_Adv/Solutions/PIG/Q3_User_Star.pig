businessData = LOAD '$business' USING PigStorage('^') AS (business_id:chararray, full_address:chararray, categories:chararray); 
reviewData = LOAD '$review' USING PigStorage('^') AS (review_id:chararray, user_id:chararray, business_id:chararray, stars:float);
userData = LOAD '$user' USING PigStorage('^') AS (user_id:chararray, name:chararray, url:chararray); 

filterData = FILTER businessData BY (full_address MATCHES '.*Stanford.*');
joinData = JOIN filterData BY business_id, reviewData BY business_id;
finalData = FOREACH joinData generate reviewData::user_id , reviewData::stars
DUMP finalData;