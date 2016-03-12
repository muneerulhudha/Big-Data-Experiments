package hadoop.algorithms.joins.yelpData_Q2_User_Ave_Rating;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class User_Ave_Rating_Driver {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();	
		FileSystem fs = FileSystem.get(conf);
		Path outputPath = new Path(args[0]);
		conf.set("userName",args[3]);
		
		if (fs.exists(outputPath))
			fs.delete(outputPath, true);
		
		Job job = Job.getInstance(conf, "Yelp User's Average Review - Reduce Side Join");
		job.setJarByClass(User_Ave_Rating_Driver.class);		

		job.setOutputKeyClass(Text.class);		
		job.setOutputValueClass(Text.class);
		
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, UserReviewMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, UserNameMapper.class);
		
		job.setReducerClass(UserReviewJoinReducer.class);
		
		//Set Input and Output Files Paths		
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, outputPath);				
				 
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}