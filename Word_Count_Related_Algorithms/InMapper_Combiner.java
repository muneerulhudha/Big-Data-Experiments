package WordCount.WordCount.StripesPackage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InMapper_Combiner{
  public static void main(String[] args) throws Exception {    
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "wordcount_InMapper_Combiner");
    FileSystem fs = FileSystem.get(conf);
    
    job.setJarByClass(InMapper_Combiner.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    if (fs.exists(new Path(args[1])))
        fs.delete(new Path(args[1]), true);
    
    job.setMapperClass(MapperClass.class);
    job.setReducerClass(Reduce.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }


  public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    private  Map<String,Integer> tokenMap;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
           tokenMap = new HashMap<String, Integer>();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        while(tokenizer.hasMoreElements()){
            String token = tokenizer.nextToken();
            Integer count = tokenMap.get(token);
            if(count == null) count = new Integer(0);
            count+=1;
            tokenMap.put(token,count);
        }
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        IntWritable writableCount = new IntWritable();
        Text text = new Text();
        Set<String> keys = tokenMap.keySet();
        for (String s : keys) {
            text.set(s);
            writableCount.set(tokenMap.get(s));
            context.write(text,writableCount);
        }
    }
  }

	public static class Reduce extends Reducer<Text, MapWritable, Text, IntWritable> {
	    private IntWritable result = new IntWritable();
	    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	      int sum = 0;
	      for (IntWritable val : values) {
	        sum += val.get();
	      }
	      result.set(sum);
	      context.write(key, result);
	    }
	}
}
