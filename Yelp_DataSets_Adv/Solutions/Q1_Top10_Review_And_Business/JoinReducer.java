package hadoop.algorithms.joins.yelpData.Q1_Top10_Review_And_Business;

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

public class JoinReducer extends Reducer<Text,Text,Text,Text> {	
	
	private Map<Text, Details> countMap = new HashMap<>();
	
	public void reduce(Text key, Iterable<Text> values,Context context ) throws IOException, InterruptedException {	
		long rating = (long) 0.0;
		String fullAddre="";
		String categories="";
		int numUsers = 0;
		
		boolean firstBusiness = true;
		for(Text instance:values){				
			String input[] = instance.toString().split("\t");	
			if(input[1].equals("Business_2") && firstBusiness){
				fullAddre = input[0];
				categories = input[2];
				firstBusiness = false;
			}
			if(input[1].equals("Review_1")){
				rating = (long) (rating + Double.parseDouble((input[0]))); //rating + Double.parseDouble(input[0]);
				++numUsers;
			}
		}	
		if(numUsers != 0)
			rating = rating/numUsers;
		Details tempItem = new Details(key.toString(), fullAddre, categories, rating);		
		countMap.put(tempItem.getBusinessID(), tempItem);		
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		Set<Entry<Text, Details>> set = countMap.entrySet();
        List<Entry<Text, Details>> list = new ArrayList<Entry<Text, Details>>(set);	       
        Collections.sort( list, new Comparator<Map.Entry<Text, Details>>()
        {
            public int compare( Map.Entry<Text, Details> o1, Map.Entry<Text, Details> o2 )
            {
                return (o2.getValue()).compareTo( o1.getValue() );
            }
        } );
        //Collections.reverseOrder();
        int counter = 0;
        
        for(Map.Entry<Text, Details> entry:list){	
        	if(counter<10){
            	context.write(new Text(entry.getKey()), new Text(entry.getValue().getAddress()+"\t"+entry.getValue().getCateogry()
            			+"\t"+entry.getValue().getRating()));
            	counter++;
        	}
        }        
	}
}