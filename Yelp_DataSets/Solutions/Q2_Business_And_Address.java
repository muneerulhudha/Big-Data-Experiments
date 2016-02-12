import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q2_Business_And_Address{
	
	public static class BusinessMap extends Mapper<LongWritable, Text, Text, Text>{
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String delims = "^";
			String[] businessData = StringUtils.split(value.toString(),delims);			
			if (businessData.length ==3) {
				if(businessData[1].contains("NY")){					
					context.write(new Text(businessData[0]), new Text(businessData[1]) );
				}
			}						
		}	
	}

	public static class Reduce extends Reducer<Text,Text,Text,Text> {		
		
		public void reduce(Text businessID, Iterable<Text> address,Context context ) throws IOException, InterruptedException {
			String adID = "";
			for(Text t : address){
				adID = adID+"   "+t;
			}		
			context.write(businessID,new Text(adID));			
		}
	}
	
// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: CountYelpBusiness <in> <out>");
			System.exit(2);
		}			  
		Job job = Job.getInstance(conf, "CountYelp");
		job.setJarByClass(Q2_Business_And_Address.class);
	   
		job.setMapperClass(BusinessMap.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);				
		
		job.setMapOutputValueClass(Text.class);
		job.setOutputValueClass(Text.class);		
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));		
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

	
	