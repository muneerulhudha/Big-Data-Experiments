package hadoop.algorithms.joins.yelpData.Q5_Business_Count_TX;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BusinessReviewMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String delims = "^";
		String[] reviewData = StringUtils.split(value.toString(),delims);
		if (reviewData.length == 4) {	
			//Write BUSINESS_ID and RATING
				context.write(new Text(reviewData[2]), new Text(reviewData[3]+"\t"+"Review_2") );
			//Output will be of the form BusinessID, Rating
		}						
	}
}
