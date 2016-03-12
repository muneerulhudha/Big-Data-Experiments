package hadoop.algorithms.joins.yelpData.Q5_Business_Count_TX;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserReviewJoinReducer extends Reducer<Text,Text,Text,Text> {
	String businessID;
	
	public void reduce(Text key, Iterable<Text> values,Context context ) throws IOException, InterruptedException {	
		long numUsers = 0;
		boolean isSelectedUser = false;
		
		for(Text instance:values){				
			String input[] = instance.toString().split("\t");
			
			if(input[1].equals("Business_1")){
				isSelectedUser = true;
				businessID=input[0];			
			}
			
			if(input[1].equals("Review_2")){																			
				++numUsers;
			}
		}	
		if(isSelectedUser && numUsers != 0){
			context.write(key, new Text(numUsers+""));
		}
	}
}
