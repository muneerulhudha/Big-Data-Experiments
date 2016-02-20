import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FindMutual {
	
	public static class UserWritable implements WritableComparable<UserWritable>{
		
		private Long userID;
		private Text name;
		private Text zipcode;
		
		public UserWritable() {
			set(new Long(-1L), new Text(), new Text());
		}
		
		public UserWritable(Long userId, String name, String zipcode){
			set(new Long(userId), new Text(name), new Text(zipcode));
		}
		
		public void set(Long userId, Text name, Text zipcode){
			this.userID = userId;
			this.name = name;
			this.zipcode = zipcode;
		}
		
		public Long getUserID() {
			return userID;
		}

		public Text getName() {
			return name;
		}

		public Text getZipcode() {
			return zipcode;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			userID = in.readLong();
			name.readFields(in);
			zipcode.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeLong(userID);
			name.write(out);
			zipcode.write(out);
		}

		@Override
		public int compareTo(UserWritable uw) {
			// TODO Auto-generated method stub
			return userID.compareTo(uw.userID); 
		}
		
	}
	
	
	public static class Map extends Mapper<LongWritable, Text, UserWritable, LongWritable>{
		
		private java.util.Map<Long, UserWritable> userDataMap = new HashMap<Long, UserWritable>();
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException{
			super.setup(context);
			Configuration conf = context.getConfiguration();
			String userdatapath = conf.get("userdatapath");
			
			Path path = new Path("hdfs://localhost:54310/user/hduser/" + userdatapath);
			
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(path);
			for(FileStatus status: fss){
				Path pt = status.getPath();
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
				String line;
				line = br.readLine();
				while(line != null){
					StringTokenizer tokenizer = new StringTokenizer(line, ",");
					Long userID = Long.parseLong(tokenizer.nextToken());
					String name = tokenizer.nextToken();
					String lname = tokenizer.nextToken();
					String address = tokenizer.nextToken();
					String city = tokenizer.nextToken();
					String state = tokenizer.nextToken();
					String zipcode = tokenizer.nextToken();
					UserWritable uw = new UserWritable(userID, name, zipcode);
					userDataMap.put(userID, uw);
					line = br.readLine();
				}
			}
			
			
		}
		
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
						UserWritable uw = userDataMap.get(friend);
						context.write(uw, new LongWritable(user));
					}
				}
			}
		}
	}
	
	public static class Reduce extends Reducer<UserWritable, LongWritable, Text, Text>{
		List<Long> output = new ArrayList<Long>();
		java.util.Map<Long, UserWritable> userDataMap = new HashMap<Long, UserWritable>();
		
		
		@Override
		public void reduce(UserWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
			
			List<Long> friends = new ArrayList<Long>();
			
			for(LongWritable val: values){
				friends.add(val.get());
			}
			
			if(friends.size() == 2){
				Long userID = key.getUserID();
				String name = key.getName().toString();
				String zipcode = key.getZipcode().toString();
				output.add(userID); 
				userDataMap.put(userID, new UserWritable(userID, name, zipcode));
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
				String name = userDataMap.get(output.get(i)).getName().toString();
				String zipcode = userDataMap.get(output.get(i)).getZipcode().toString();
				if(i == 0)
					out = "[" + name + ":" + zipcode;
				else if(i == (output.size() -1))
					out += ", " + name + ":" + zipcode + "]";
				else
					out += ", " + name + ":" + zipcode;
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
		conf.set("userdatapath", "input/userdata.txt");
		
		Job job = new Job(conf, "FindMutual");
		job.setJarByClass(FindMutual.class);
		
		job.setOutputKeyClass(UserWritable.class);
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
