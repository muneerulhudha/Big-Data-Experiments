package hadoop.algorithms.joins.yelpData.Q3_User_Rating_Stanford;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserReviewMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String delims = "^";
		String[] reviewData = StringUtils.split(value.toString(),delims);
		if (reviewData.length == 4) {	
			context.write(new Text(reviewData[2]), new Text(reviewData[1]+"\t"+"Review_1"+"\t"+reviewData[3]) );
			//BUSINESS_ID		USER_ID		STARS
		}						
	}
}
