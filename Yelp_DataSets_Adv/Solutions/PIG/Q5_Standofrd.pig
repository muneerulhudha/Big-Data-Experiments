businessData = LOAD '$business' USING PigStorage('^') AS (business_id:chararray, full_address:chararray, categories:chararray); 
reviewData = LOAD '$review' USING PigStorage('^') AS (review_id:chararray, user_id:chararray, business_id:chararray, stars:float);
userData = LOAD '$user' USING PigStorage('^') AS (user_id:chararray, name:chararray, url:chararray); 

filterData = FILTER businessData BY (full_address MATCHES '.* TX .*');
joinedData = JOIN filterData BY business_id, reviewData BY business_id;
groupedData = FOREACH (GROUP joinedData BY filterData::business_id){
	GENERATE group AS filterData::business_id, COUNT(joinedData.filterData::business_id) AS ratingsCount;
};
orderData = ORDER groupedData BY ratingsCount DESC;
topTen = LIMIT orderData 10;
DUMP topTen;
