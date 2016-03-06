
import java.io.IOException;
import java.text.*;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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



public class Q6_Last_10_Ave_Rating{	
	public static class ReviewMap extends Mapper<LongWritable, Text, Text, DoubleWritable>{

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//from reviews
			String delims = "//^";
			String[] reviewData = StringUtils.split(value.toString(),delims);
			if (reviewData.length ==4) {		
				double rating = Double.parseDouble(reviewData[3]);
				context.write(new Text(reviewData[2]), new DoubleWritable(rating));
			}		
		}
	}
		
	public static class ReviewReduce extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
		
		private Map<String, Double> countMap = new HashMap<>();

		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values,Context context ) throws IOException, InterruptedException {
			
			int count=0;
			double sum = 0.0;
			
			for (DoubleWritable val : values) {
				sum += val.get();
				count++;
			}
			Double avg =  ((double)sum/(double)count);
			//context.write(key, new DoubleWritable(avg));
			countMap.put(key.toString(), avg);
		}
		
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			Set<Entry<String, Double>> set = countMap.entrySet();
	        List<Entry<String, Double>> list = new ArrayList<Entry<String, Double>>(set);	       
	        Collections.sort( list, new Comparator<Map.Entry<String, Double>>()
	        {
	            public int compare( Map.Entry<String, Double> o1, Map.Entry<String, Double> o2 )
	            {
	                return (o2.getValue()).compareTo( o1.getValue() );
	            }
	        } );
	        Collections.reverseOrder();
	        int counter = 0;
	        int size = list.size()-1;
	        for(Map.Entry<String, Double> entry:list){	
	        	if(size - counter<10){
	            	context.write(new Text(entry.getKey()), new DoubleWritable(entry.getValue()));	            	
	        	}
	        	counter++;
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
		
		Job job = Job.getInstance(conf, "LastAveRating");
		job.setJarByClass(Q6_Last_10_Ave_Rating.class);
	   
		job.setMapperClass(ReviewMap.class);
		job.setReducerClass(ReviewReduce.class);

		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);		
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));		
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		job.waitForCompletion(true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

	
	