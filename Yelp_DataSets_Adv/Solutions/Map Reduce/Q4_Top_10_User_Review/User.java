package hadoop.algorithms.joins.yelpData.Q4_Top_10_User_Review;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class User implements WritableComparable<User>{

	private Text userName;
	private LongWritable numOfReviews;
	private Text userID;
		
	public Text getUserName() {
		return userName;
	}

	public Text getUserID() {
		return userID;
	}

	
	public LongWritable getNumOfReviews() {
		return numOfReviews;
	}

	User(){
		userName   = new Text();
		numOfReviews = new LongWritable();
	}
	
	User(String bID, long numOfReviews, String userID){
		this.userName = new Text(bID);
		this.userID = new Text(userID);
		this.numOfReviews = new LongWritable(numOfReviews);
	}
	
	public void set(String bID, String addr, String cat, long rating){
		this.userName = new Text(bID);
		this.numOfReviews =  new LongWritable(rating);
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		userName.write(out);
		numOfReviews.write(out);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		userName.readFields(in);
		numOfReviews.readFields(in);
	}

	@Override
	public int compareTo(User arg0) {
		// TODO Auto-generated method stub
		int cmp = numOfReviews.compareTo(arg0.numOfReviews);
		if(cmp != 0){
			return cmp;
		}
		return numOfReviews.compareTo(arg0.numOfReviews);
	}
	
	@Override
	public boolean equals(Object o){
		if(o instanceof User){
			User tp = (User) o;
			return numOfReviews.equals(tp.numOfReviews) && userName.equals(tp.userName);
		}
		return false;
	}
	
	@Override
	public int hashCode(){
		return userName.hashCode() * 154 + numOfReviews.hashCode();
	}
	
}
