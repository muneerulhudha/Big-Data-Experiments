
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;

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



public class Q3_Top_10_Zip{	
	public static class ReviewMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		private final static IntWritable one = new IntWritable(1);
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String delims = "//^";
			String address;
			String zipcode;
			String[] businessData = StringUtils.split(value.toString(),delims);
			if (businessData.length ==3) {
				address = businessData[1];
				zipcode = address.substring(address.length()-6, address.length());
				context.write(new Text(zipcode), one);	
			}	
		}
	}
		
	public static class ReviewReduce extends Reducer<Text,IntWritable,Text,IntWritable> {
		
		private Map<String, Integer> countMap = new HashMap<>();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,Context context ) throws IOException, InterruptedException {
						
			int sum = 0;
			
			for (IntWritable val : values) {
				sum += val.get();
			}
			
			//context.write(key, new IntWritable(sum));
			countMap.put(key.toString(), sum);
		}
		
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			Set<Entry<String, Integer>> set = countMap.entrySet();
	        List<Entry<String, Integer>> list = new ArrayList<Entry<String, Integer>>(set);	       
	        Collections.sort( list, new Comparator<Map.Entry<String, Integer>>()
	        {
	            public int compare( Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2 )
	            {
	                return (o2.getValue()).compareTo( o1.getValue() );
	            }
	        } );
	          
	        int counter = 0;
	        for(Map.Entry<String, Integer> entry:list){	
	        	if(counter<10){
	            	context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
	            	counter++;
	        	}
	        }        
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();		
		if (otherArgs.length != 2) {
			System.err.println("Usage: CountYelpReview <in> <out>");
			System.exit(2);
		}
		
		Job job = Job.getInstance(conf, "TopAveRating");
		job.setJarByClass(Q3_Top_10_Zip.class);
	   
		job.setMapperClass(ReviewMap.class);
		job.setReducerClass(ReviewReduce.class);

		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);		
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));		
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		job.waitForCompletion(true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

	
	