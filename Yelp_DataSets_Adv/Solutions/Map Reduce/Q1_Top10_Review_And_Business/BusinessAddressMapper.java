package hadoop.algorithms.joins.yelpData.Q1_Top10_Review_And_Business;

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
			//Write BUSINESS_ID and FULL_ADDRESS with Category
			context.write(new Text(businessData[0]), new Text(businessData[1]+"\t"+"Business_2"+"\t"+businessData[2]) );				
			//Output will be of the form BUSINESSID, FULLADDR \t CATEGORIES
		}
	}		
}
