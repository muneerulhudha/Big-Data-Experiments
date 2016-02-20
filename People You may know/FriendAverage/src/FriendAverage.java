import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class FriendAverage {
	
	private static final String OUTPUT_PATH = "/intermediate_output";
	
	public static class TextPair implements WritableComparable<TextPair>{
		private Text first;
		private Text second;
		
		public TextPair(){
			set(new Text(), new Text());
		}
		
		public TextPair(Text first, Text second){
			set(first, second);
		}
		
		public TextPair(String first, String second){
			set(new Text(first), new Text(second));
		}
		
		public void set(Text first, Text second){
			this.first = first;
			this.second = second;
		}
		
		public Text getFirst(){
			return first;
		}
		
		public Text getSecond(){
			return second;
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			first.readFields(in);
			second.readFields(in);
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			first.write(out);
			second.write(out);
		}
		
		@Override
		public int hashCode(){
			return first.hashCode() * 154 + second.hashCode();
		}
		
		@Override
		public boolean equals(Object o){
			if(o instanceof TextPair){
				TextPair tp = (TextPair) o;
				return first.equals(tp.first) && second.equals(tp.second);
			}
			return false;
		}
		
		@Override
		public int compareTo(TextPair tp) {
			// TODO Auto-generated method stub
			int cmp = first.compareTo(tp.first);
			if(cmp != 0){
				return cmp;
			}
			return second.compareTo(tp.second);
		}

		public static int compare(Text first2, Text first3) {
			// TODO Auto-generated method stub
			return 0;
		}
		
	}	
	
	public static class Mapper1 extends Mapper<LongWritable, Text, LongWritable, LongWritable>{
		
		private java.util.Map<Long, Long> userAgeMap = new HashMap<Long, Long>();
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException{
			super.setup(context);
			Configuration conf = context.getConfiguration();
			String userdatapath = conf.get("userdatapath");
			
			Path path = new Path("hdfs://localhost:54310/user/hduser/" + userdatapath);

			SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");
			
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(path);
			for(FileStatus status: fss){
				Path pt = status.getPath();
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
				String line;
				line = br.readLine();
				while(line != null){
					String lineArr[] = line.split(",");
					Long userID = Long.parseLong(lineArr[0]);
					String dob = lineArr[9];
					
//					StringTokenizer tokenizer = new StringTokenizer(line, ",");
//					Long userID = Long.parseLong(tokenizer.nextToken());
//					String name = tokenizer.nextToken();
//					String lname = tokenizer.nextToken();
//					String address = tokenizer.nextToken();
//					String city = tokenizer.nextToken();
//					String state = tokenizer.nextToken();
//					String zipcode = tokenizer.nextToken();
//					String country = tokenizer.nextToken();
//					//String username = tokenizer.nextToken();
//					String dob = tokenizer.nextToken();

					Date date = null;
					try {
						date = formatter.parse(dob);
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					Calendar dateofbirth = Calendar.getInstance();  
					dateofbirth.setTime(date);  
					Calendar today = Calendar.getInstance();  
					int age = today.get(Calendar.YEAR) - dateofbirth.get(Calendar.YEAR);  
					if (today.get(Calendar.MONTH) < dateofbirth.get(Calendar.MONTH)) {
					  age--;  
					} else if (today.get(Calendar.MONTH) == dateofbirth.get(Calendar.MONTH)
					    && today.get(Calendar.DAY_OF_MONTH) < dateofbirth.get(Calendar.DAY_OF_MONTH)) {
					  age--;  
					}

					Long ageLong = new Long(age);
					userAgeMap.put(userID, ageLong);
					line = br.readLine();
				}
			}
			
			
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line[] = value.toString().split("\t");
			Long user = Long.parseLong(line[0]);
			
			Configuration conf = context.getConfiguration();
			
			if(line.length == 2) {
				StringTokenizer tokenizer = new StringTokenizer(line[1], ",");
				while(tokenizer.hasMoreTokens()){
					Long age = userAgeMap.get(Long.parseLong(tokenizer.nextToken()));
					context.write(new LongWritable(user), new LongWritable(age));
				}
			}
		}
	}
	
	public static class Reducer1 extends Reducer<LongWritable, LongWritable, Text, Text>{
		List<Long> output = new ArrayList<Long>();
		java.util.Map<String, Long> userAgeMap = new HashMap<String, Long>();
		
		@Override
		public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
			
			List<Long> ages = new ArrayList<Long>();
			Long count = (long) 0;
			Long sum = (long) 0;
			
			for(LongWritable val: values){
				ages.add(val.get());
			}
			
			for(Long age : ages){
				sum = sum + age;
				count = count + 1;
			}
			
			Long average = sum/count;
			
			context.write(new Text(key.toString()), new Text(average.toString()));
		}
		
	}

	public static class JoinMapperAge extends Mapper<LongWritable, Text, TextPair, Text>{
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line[] = value.toString().split("\t");
			Long user = Long.parseLong(line[0]);
			Long age = Long.parseLong(line[1]);
			
			context.write(new TextPair(user.toString(), "0"), new Text(age.toString()));
		}
		
	}
	
	public static class JoinMapperAddress extends Mapper<LongWritable, Text, TextPair, Text>{
				
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer tokenizer = new StringTokenizer(value.toString(), ",");
			
			String userID = tokenizer.nextToken();
			String name = tokenizer.nextToken();
			String lname = tokenizer.nextToken();
			String address = tokenizer.nextToken();
			String city = tokenizer.nextToken();
			String state = tokenizer.nextToken();

			String addressFull = address + ", " + city + ", " + state; 
			
			context.write(new TextPair(userID, "1"), new Text(addressFull));
		}
		
	}
	
	public static class JoinReducer extends Reducer<TextPair, Text, Text, Text>{
		
		java.util.Map<String, Long> userAgeMap = new HashMap<String, Long>();
		
		public LinkedHashMap sortHashMapByValuesD(Map<String, Long> userAgeMap2) {
			   List mapKeys = new ArrayList(userAgeMap2.keySet());
			   List mapValues = new ArrayList(userAgeMap2.values());
			   Collections.sort(mapValues);
			   Collections.sort(mapKeys);

			   LinkedHashMap sortedMap = new LinkedHashMap();

			   Iterator valueIt = mapValues.iterator();
			   while (valueIt.hasNext()) {
			       Object val = valueIt.next();
			       Iterator keyIt = mapKeys.iterator();

			       while (keyIt.hasNext()) {
			           Object key = keyIt.next();
			           String comp1 = userAgeMap2.get(key).toString();
			           String comp2 = val.toString();

			           if (comp1.equals(comp2)){
			               userAgeMap2.remove(key);
			               mapKeys.remove(key);
			               sortedMap.put((String)key, (Long)val);
			               break;
			           }
			       }

			   }
			   return sortedMap;
		}
		
		@Override
		public void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			Iterator<Text> iter = values.iterator();
			Text avgAge = new Text(iter.next());
			while(iter.hasNext()){
				Text address = new Text(iter.next());
				Text output = new Text(key.getFirst() + ", " + address.toString() + ", ");
				
				userAgeMap.put(output.toString(), Long.parseLong(avgAge.toString()));
				
				//context.write(key.getFirst(), output);
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
	
			LinkedHashMap sortedMap = sortHashMapByValuesD(userAgeMap);
			
			List<Entry<String,Long>> list = new ArrayList<>(sortedMap.entrySet());

			for( int i = list.size() -1; i >= list.size() - 21 ; i --){
			    Entry<String,Long> me = list.get(i);
			    context.write(new Text(me.getKey().toString()), new Text(me.getValue().toString()));
			}
			
//			Set set = sortedMap.entrySet();
//		    Iterator it = set.iterator();
//		    while(it.hasNext()) {
//		         Map.Entry me = (Map.Entry)it.next();
//		         context.write(new Text(me.getKey().toString()), new Text(me.getValue().toString()));
//		    }
			
		}
		
	}

	public static class KeyPartitioner extends Partitioner<TextPair, Text>{

		@Override
		public int getPartition(TextPair key, Text value, int numOfPartitions) {
			// TODO Auto-generated method stub
			return(key.getFirst().hashCode() & Integer.MAX_VALUE) % numOfPartitions;
		}
		
	}
	
	public static class GroupComparator extends WritableComparator{
		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
		
		protected GroupComparator(){
			super(TextPair.class, true);
		}
		
		@Override
		public int compare(WritableComparable w1, WritableComparable w2){
			TextPair tp1 = (TextPair) w1;
			TextPair tp2 = (TextPair) w2;
			return TEXT_COMPARATOR.compare(tp1.getFirst(), tp2.getFirst());
		}
	}
	
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		conf.set("userdatapath", "input/userdata.txt");
		
		Path path = new Path("hdfs://localhost:54310/user/hduser/input/userdata.txt");
		
		Job job1 = new Job(conf, "FindAverage");
		job1.setJarByClass(FriendAverage.class);
		
		job1.setOutputKeyClass(LongWritable.class);
		job1.setOutputValueClass(LongWritable.class);
		
		job1.setMapperClass(Mapper1.class);
		job1.setReducerClass(Reducer1.class);
		
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		FileSystem outFs = new Path(OUTPUT_PATH).getFileSystem(conf);
		outFs.delete(new Path(OUTPUT_PATH), true);
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(OUTPUT_PATH));
		
		job1.waitForCompletion(true);
		
		Job job2 = new Job(conf, "FindAverage");
		job2.setJarByClass(FriendAverage.class);
		
		MultipleInputs.addInputPath(job2, new Path(OUTPUT_PATH), TextInputFormat.class, JoinMapperAge.class);
		MultipleInputs.addInputPath(job2, path, TextInputFormat.class, JoinMapperAddress.class);
		
		FileSystem outFs2 = new Path(args[1]).getFileSystem(conf);
		outFs2.delete(new Path(args[1]), true);
		
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		job2.setPartitionerClass(KeyPartitioner.class);
		job2.setGroupingComparatorClass(GroupComparator.class);
		
		job2.setMapOutputKeyClass(TextPair.class);
		
		job2.setReducerClass(JoinReducer.class);
		
		job2.setOutputKeyClass(Text.class);
		
		job2.waitForCompletion(true);
		
	}

}
