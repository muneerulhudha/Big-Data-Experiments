package hadoop.algorithms.joins.yelpData.Q1_Top10_Review_And_Business;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Q1 {	
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();	
		FileSystem fs = FileSystem.get(conf);
		Path outputPath = new Path("UJG/OP1");
		
		if (fs.exists(outputPath))
			fs.delete(outputPath, true);
		
		Job job = Job.getInstance(conf, "Yelp Business to Review - Reduce Side Join");
		job.setJarByClass(Q1.class);		
		
		//Set Output Types
		job.setOutputKeyClass(Text.class);		
		job.setOutputValueClass(Text.class);
		
		MultipleInputs.addInputPath(job, new Path("UJG/yelpdatafall/business/business.csv"), TextInputFormat.class, BusinessAddressMapper.class);
		MultipleInputs.addInputPath(job, new Path("UJG/yelpdatafall/review/review.csv"), TextInputFormat.class, ReviewMapper.class);
				
		job.setReducerClass(JoinReducer.class);
		
		//Set Input and Output Files Paths		
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, outputPath);				
				 
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
