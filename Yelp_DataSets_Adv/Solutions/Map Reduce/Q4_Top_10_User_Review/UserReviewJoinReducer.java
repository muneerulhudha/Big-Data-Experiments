package hadoop.algorithms.joins.yelpData.Q4_Top_10_User_Review;

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
	
	private Map<Text, User> countMap = new HashMap<>();
	String userName;
	
	public void reduce(Text key, Iterable<Text> values,Context context ) throws IOException, InterruptedException {	
		long numUsers = 0;
		boolean isSelectedUser = false;
		
		for(Text instance:values){				
			String input[] = instance.toString().split("\t");
			
			if(input[1].equals("User_2")){
				isSelectedUser = true;
				userName=input[0];			
			}
			
			if(input[1].equals("Review_1")){																			
				++numUsers;
			}
		}	
		User newUser = new User(userName,numUsers,key.toString());
		
		if(isSelectedUser && numUsers != 0){
			countMap.put(new Text(userName), newUser);
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		Set<Entry<Text, User>> set = countMap.entrySet();
        List<Entry<Text, User>> list = new ArrayList<Entry<Text, User>>(set);	       
        Collections.sort( list, new Comparator<Map.Entry<Text, User>>()
        {
            public int compare( Map.Entry<Text, User> o1, Map.Entry<Text, User> o2 )
            {
                return (o2.getValue()).compareTo( o1.getValue() );
            }
        } );
        //Collections.reverseOrder();
        int counter = 0;
        
        for(Map.Entry<Text, User> entry:list){	
        	if(counter<10){
            	context.write(new Text(entry.getValue().getUserID().toString()), new Text(entry.getValue().getUserName().toString()));
            	counter++;
        	}
        }        
	}
}
