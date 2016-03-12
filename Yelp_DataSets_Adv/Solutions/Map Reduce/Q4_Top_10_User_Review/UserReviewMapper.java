package hadoop.algorithms.joins.yelpData.Q4_Top_10_User_Review;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserReviewMapper extends Mapper<LongWritable, Text, Text, Text>{
		
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String delims = "^";
		String[] reviewData = StringUtils.split(value.toString(),delims);
		if (reviewData.length == 4) {	
			//Write BUSINESS_ID and RATING
			context.write(new Text(reviewData[1]), new Text(reviewData[3]+"\t"+"Review_1") );
			//Output will be of the form USERID, RATING
		}						
	}
}
