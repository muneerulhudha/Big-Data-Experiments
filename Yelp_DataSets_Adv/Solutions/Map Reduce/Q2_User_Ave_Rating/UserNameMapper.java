package hadoop.algorithms.joins.yelpData_Q2_User_Ave_Rating;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserNameMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String delims = "^";
		String[] userData = StringUtils.split(value.toString(),delims);
		if (userData.length == 3) {	
			//Write BUSINESS_ID and RATING
				context.write(new Text(userData[0]), new Text(userData[1]+"\t"+"User_2") );
			//Output will be of the form USERID, USERNAME
		}						
	}
}
