businessData = LOAD '$business' USING PigStorage('^') AS (business_id:chararray, full_address:chararray, categories:chararray); 
reviewData = LOAD '$review' USING PigStorage('^') AS (review_id:chararray, user_id:chararray, business_id:chararray, stars:float);
userData = LOAD '$user' USING PigStorage('^') AS (user_id:chararray, name:chararray, url:chararray); 

filterData = FILTER userData BY (name matches '.*$id.*');
joinData = JOIN filterData BY user_id, reviewData BY user_id;
groupedData = FOREACH (GROUP joinData BY filterData::name) {
        GENERATE group AS filterData::name,AVG(joinData.reviewData::stars) AS ave_rating;
};
DUMP groupedData;
