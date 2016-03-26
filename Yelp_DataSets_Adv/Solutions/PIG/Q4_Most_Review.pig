businessData = LOAD '$business' USING PigStorage('^') AS (business_id:chararray, full_address:chararray, categories:chararray); 
reviewData = LOAD '$review' USING PigStorage('^') AS (review_id:chararray, user_id:chararray, business_id:chararray, stars:float);
userData = LOAD '$user' USING PigStorage('^') AS (user_id:chararray, name:chararray, url:chararray); 

joinData = JOIN reviewData BY user_id, userData BY user_id;
groupedData = FOREACH (GROUP joinData BY reviewData::user_id) {
        b = joinData.userData::name;
        data = DISTINCT b;
        GENERATE group AS reviewData::user_id, FLATTEN(data) AS name, COUNT(joinData.reviewData::user_id) AS totalCount;
};
orderData = ORDER groupedData BY totalCount DESC;
topTen = LIMIT orderData 10;
DUMP topTen;
