import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q4_Top_10_HashTags {

  public static class tweetMapper extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();      
      if(line.startsWith("UJG123469")){    	  
    	  word.set(line.split(",")[1]);
    	  context.write(word, one);
      }      
    }
  }

  public static class tweetReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	  
	  private Map<String, Integer> countMap = new HashMap<>();
	  private IntWritable result = new IntWritable();

	  public void reduce(Text key, Iterable<IntWritable> values,Context context)
			  throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      countMap.put(key.toString(), sum);
    }
    
    @Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		Set<Entry<String, Integer>> set = countMap.entrySet();
        List<Entry<String, Integer>> list = new ArrayList<Entry<String, Integer>>(set);
        int counter = 0;
        
        Collections.sort( list, new Comparator<Map.Entry<String, Integer>>()
        {
            public int compare( Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2 )
            {
                return (o2.getValue()).compareTo( o1.getValue() );
            }
        });       
        
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
    
    Job job = Job.getInstance(conf, "Tweet Top Ten Word Count");
    job.setJarByClass(Q4_Top_10_HashTags.class);
    job.setMapperClass(tweetMapper.class);
    job.setCombinerClass(tweetReducer.class);
    job.setReducerClass(tweetReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}