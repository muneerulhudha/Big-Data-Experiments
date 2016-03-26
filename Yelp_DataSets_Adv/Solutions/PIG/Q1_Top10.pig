businessData = LOAD '$business' USING PigStorage('^') AS (business_id:chararray, full_address:chararray, categories:chararray); 
reviewData = LOAD '$review' USING PigStorage('^') AS (review_id:chararray, user_id:chararray, business_id:chararray, stars:float);
userData = LOAD '$user' USING PigStorage('^') AS (user_id:chararray, name:chararray, url:chararray); 

joinData = JOIN businessData BY business_id, reviewData BY business_id;
groupedData = FOREACH (GROUP joinData BY businessData::business_id) {
	b = joinData.(businessData::full_address, businessData::categories);
        data = DISTINCT b;
        GENERATE group AS businessData::business_id, FLATTEN(data),AVG(joinData.stars) AS ave_rating;
};
orderData = ORDER groupedData BY ave_rating DESC;
topTen = LIMIT orderData 10;
DUMP topTen;
