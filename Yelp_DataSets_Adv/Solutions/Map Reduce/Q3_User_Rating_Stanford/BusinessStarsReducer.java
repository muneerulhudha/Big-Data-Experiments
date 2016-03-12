package hadoop.algorithms.joins.yelpData.Q3_User_Rating_Stanford;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BusinessStarsReducer extends Reducer<Text,Text,Text,Text>{
	
	public void reduce(Text key, Iterable<Text> values,Context context ) throws IOException, InterruptedException {	
		
		String rating = "";
		String userID = "";
		boolean stanRecord = false;

		for(Text instance:values){				
			String input[] = instance.toString().split("\t");			
			if(input[1].equals("Business_1")){
				stanRecord = true;
			}
			
			if(input[1].equals("Review_1")){							
				rating = input[0];
				userID = input[2];				
			}
		}
		
		if(stanRecord){
			context.write(new Text(rating), new Text(userID));
		}
	}
}
