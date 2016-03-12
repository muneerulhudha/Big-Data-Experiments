package hadoop.algorithms.joins.yelpData.Q3_User_Rating_Stanford;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BusinessAddressMapper extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String delims = "^";
		String[] businessData = StringUtils.split(value.toString(),delims);
		if (businessData.length == 3) {	
			//Write BUSINESS_ID and RATING
			if(businessData[1].contains("stanford") || businessData[1].contains("Stanford"))
				context.write(new Text(businessData[0]), new Text(businessData[1]+"\t"+"Business_1") );
			//Output will be of the form BUSINESS_ID, ADDRESS
		}						
	}
}
