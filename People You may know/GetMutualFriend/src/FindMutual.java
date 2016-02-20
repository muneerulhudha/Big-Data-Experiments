import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FindMutual {
	
	public static class Map extends Mapper<LongWritable, Text, LongWritable, LongWritable>{
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line[] = value.toString().split("\t");
			Long user = Long.parseLong(line[0]);
			
			Configuration conf = context.getConfiguration();
			if(user == conf.getLong("user1", -1L) || user == conf.getLong("user2", -1L)){
				if(line.length == 2){
					StringTokenizer tokenizer = new StringTokenizer(line[1], ",");
					while(tokenizer.hasMoreTokens()){
						Long friend = Long.parseLong(tokenizer.nextToken());
						context.write(new LongWritable(friend), new LongWritable(user));
					}
				}
			}
		}
	}
	
	public static class Reduce extends Reducer<LongWritable, LongWritable, Text, Text>{
		List<Long> output = new ArrayList<Long>();
		
		@Override
		public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
			
			List<Long> friends = new ArrayList<Long>();
			
			for(LongWritable val: values){
				friends.add(val.get());
			}
			
			if(friends.size() == 2){
				Long user = key.get();
				output.add(user); 
			}
				
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			Long user1 = conf.getLong("user1", -1L);
			Long user2 = conf.getLong("user2", -1L);
			
			String input = user1.toString() + " " + user2.toString();
			String out ="";
			
			for(int i=0; i<output.size(); i++){
				if(i == 0)
					out = output.get(i).toString();
				else
					out += ", " + output.get(i);
			}
			
			context.write(new Text(input), new Text(out));
		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Long user1 = Long.parseLong(args[2]);
		Long user2 = Long.parseLong(args[3]);
		conf.setLong("user1", user1);
		conf.setLong("user2", user2);
		
		Job job = new Job(conf, "FindMutual");
		job.setJarByClass(FindMutual.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileSystem outFs = new Path(args[1]).getFileSystem(conf);
		outFs.delete(new Path(args[1]), true);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}
	
}
