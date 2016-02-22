package WordCount.WordCount.StripesPackage;

import java.io.IOException;
import java.util.Set;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class CoOccurence extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(CoOccurence.class);
	
	  public static void main(String[] args) throws Exception {
	    int res = ToolRunner.run(new CoOccurence(), args);
	    System.exit(res);
	  }

	  public int run(String[] args) throws Exception {
	    Job job = Job.getInstance(getConf(), "wordcount");
	    FileSystem fs = FileSystem.get(getConf());
	    for (int i = 0; i < args.length; i += 1) {
	      if ("-skip".equals(args[i])) {
	        job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
	        i += 1;
	        job.addCacheFile(new Path(args[i]).toUri());
	        LOG.info("Added file to the distributed cache: " + args[i]);
	      }
	    }
	    job.setJarByClass(this.getClass());
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
		job.addCacheFile(new Path(args[3]).toUri());
	    job.getConfiguration().set("Length", args[2]);
		
	    job.setMapperClass(CoOccurenceMapper.class);
	    job.setMapOutputKeyClass(Text.class);
	    
	    job.setMapOutputValueClass(MapWritable.class);
	    job.setReducerClass(CoOccurenceReduce.class);

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    if (fs.exists(new Path(args[1])))
	        fs.delete(new Path(args[1]), true);
	    
	    
	    return job.waitForCompletion(true) ? 0 : 1;
	  }

	  	 
	}



