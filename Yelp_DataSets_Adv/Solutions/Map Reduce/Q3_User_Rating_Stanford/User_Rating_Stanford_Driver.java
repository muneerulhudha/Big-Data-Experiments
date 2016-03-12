package hadoop.algorithms.joins.yelpData.Q3_User_Rating_Stanford;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class User_Rating_Stanford_Driver {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();	
		FileSystem fs = FileSystem.get(conf);
		Path outputPath = new Path(args[0]);
		
		if (fs.exists(outputPath))
			fs.delete(outputPath, true);
		
		Job job = Job.getInstance(conf, "Yelp User's Average Review - Reduce Side Join");
		job.setJarByClass(User_Rating_Stanford_Driver.class);		

		job.setOutputKeyClass(Text.class);		
		job.setOutputValueClass(Text.class);
		
		//USER_ID    STARS
		//
		//BUSINESS_ID 		FULL_ADDRESS
		//BUSINESS_ID		USER_ID		STARS
		
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, UserReviewMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, BusinessAddressMapper.class);
				
		job.setReducerClass(BusinessStarsReducer.class);
		
		//Set Input and Output Files Paths		
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, outputPath);				
				 
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}