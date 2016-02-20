import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FriendReco {

	static public class FriendWritable implements Writable{
		public Long user;
		public Long friend;
		
		public FriendWritable(Long user, Long friend){
			this.user = user;
			this.friend = friend;
		}
		
		public FriendWritable(){
			this(-1L, -1L);
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			user = in.readLong();
			friend = in.readLong();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeLong(user);
			out.writeLong(friend);
		}
		
		@Override
		public String toString(){
			return "user: " + Long.toString(user) + " friend: " + Long.toString(friend);
		}	
	}
	
	public static class Map extends Mapper<LongWritable, Text, LongWritable, FriendWritable>{
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line[] = value.toString().split("\t");
			Long user = Long.parseLong(line[0]);
			List<Long> friends = new ArrayList<Long>();
			
			if(line.length < 2){
				context.write(new LongWritable(user), new FriendWritable());
			}
			
			if(line.length == 2){
				StringTokenizer tokenizer = new StringTokenizer(line[1], ",");
				while(tokenizer.hasMoreTokens()){
					Long friend = Long.parseLong(tokenizer.nextToken());
					friends.add(friend);
					context.write(new LongWritable(user), new FriendWritable(friend, -1L));
				}
				
				for(int i = 0; i <friends.size(); i++){
					for(int j=i; j<friends.size(); j++){
						context.write(new LongWritable(friends.get(i)), new FriendWritable((friends.get(j)), user));
						context.write(new LongWritable(friends.get(j)), new FriendWritable((friends.get(i)), user));
					}
				}
			}	
		}
	}
	
	public static class Reduce extends Reducer<LongWritable, FriendWritable, LongWritable, Text>{
		
		@Override
		public void reduce(LongWritable key, Iterable<FriendWritable> values, Context context) throws IOException, InterruptedException{
			
			final java.util.Map<Long, List<Long>> mutualFriends = new HashMap<Long, List<Long>>();
			if(values == null)
				context.write(key, new Text(""));
			else{
				for(FriendWritable val: values){
					final Boolean isFriend = (val.friend == -1);
					final Long toUser = val.user;
					final Long mutualFriend = val.friend;
					
					if(mutualFriends.containsKey(toUser)){
						if(isFriend){
							mutualFriends.put(toUser, null);
						}else if(mutualFriends.get(toUser) != null){
							mutualFriends.get(toUser).add(mutualFriend);						
						}
					}else{
						if(!isFriend){
							mutualFriends.put(toUser, new ArrayList<Long>() {
								{
									add(mutualFriend);
								}
							});
						}else{
							mutualFriends.put(toUser, null);
						}
					}
				}
				
				java.util.SortedMap<Long, List<Long>> sortedMutualFriends = new TreeMap<Long, List<Long>>(new Comparator<Long>(){
	
					@Override
					public int compare(Long arg0, Long arg1) {
						Integer v1 = mutualFriends.get(arg0).size();
						Integer v2 = mutualFriends.get(arg1).size();
						
						if(v1>v2){
							return -1;
						}else if(v1.equals(v2) && arg0 < arg1){
							return -1;
						}else{
							return 1;
						}
					}
					
				});
				
				for(java.util.Map.Entry<Long, List<Long>> entry : mutualFriends.entrySet()){
					if(entry.getValue() != null){
						sortedMutualFriends.put(entry.getKey(), entry.getValue());
					}
				}
				
				Integer i = 0;
				String output = "";
				
				for(java.util.Map.Entry<Long, List<Long>> entry: sortedMutualFriends.entrySet()){
					if(i==1){
						output = entry.getKey().toString() + " ";
					}else if(i < 11){
						output += "," + entry.getKey().toString() + " ";
					}
					++i;
				}
				
				context.write(key, new Text(output));
			}	
		}	
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "FriendReco");
		job.setJarByClass(FriendReco.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(FriendWritable.class);
		
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
