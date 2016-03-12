package hadoop.algorithms.joins.yelpData_Q2_User_Ave_Rating;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserReviewJoinReducer extends Reducer<Text,Text,Text,Text> {
	
	ArrayList<Double> userRating = new ArrayList<>();
	String userName;
	
	public void reduce(Text key, Iterable<Text> values,Context context ) throws IOException, InterruptedException {	
		int numUsers = 0;
		boolean isSelectedUser = false;
		double rating = 0.0;
		Configuration conf = context.getConfiguration();
		String username = conf.get("userName");
		
		for(Text instance:values){				
			String input[] = instance.toString().split("\t");
			
			if(input[1].equals("User_2") && input[0].equals(username)){
				isSelectedUser = true;
				userName=input[0];			
			}
			
			if(input[1].equals("Review_1")){							
				rating = (long) (rating + Double.parseDouble((input[0])));												
				++numUsers;
			}
		}	
		
		if(isSelectedUser && numUsers != 0){
			rating  = (double) rating/numUsers;
			userRating.add( Double.parseDouble(rating+""));
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		double rating = 0.0;	
		int size = userRating.size();
		for(Double i : userRating){
			rating = rating + i;			
		}		
		if(size != 0){
			rating = (double) rating / (long)size;
			context.write(new Text(userName), new Text(rating+""));
		}
	}
}
