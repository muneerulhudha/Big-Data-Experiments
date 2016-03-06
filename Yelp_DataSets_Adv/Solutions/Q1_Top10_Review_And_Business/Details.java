package hadoop.algorithms.joins.yelpData.Q1_Top10_Review_And_Business;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Details implements WritableComparable<Details>{

	private Text businessID;
	private Text address;
	private Text cateogry;
	private LongWritable rating;
	
	public Text getBusinessID() {
		return businessID;
	}

	public Text getAddress() {
		return address;
	}

	public Text getCateogry() {
		return cateogry;
	}

	public LongWritable getRating() {
		return rating;
	}

	Details(){
		businessID   = new Text();
		address  = new Text();
		cateogry = new Text();
		rating = new LongWritable();
	}
	
	Details(String bID, String addr, String cat, long rating){
		this.businessID = new Text(bID);
		this.address = new Text(addr) ;
		this.cateogry = new Text(cat);
		this.rating = new LongWritable(rating);
	}
	
	public void set(String bID, String addr, String cat, long rating){
		this.businessID = new Text(bID);
		this.address = new Text(addr) ;
		this.cateogry = new Text(cat);
		this.rating =  new LongWritable(rating);
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		businessID.write(out);
		address.write(out);
		cateogry.write(out);
		rating.write(out);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		businessID.readFields(in);
		address.readFields(in);
		cateogry.readFields(in);
		rating.readFields(in);
	}

	@Override
	public int compareTo(Details arg0) {
		// TODO Auto-generated method stub
		int cmp = rating.compareTo(arg0.rating);
		if(cmp != 0){
			return cmp;
		}
		return rating.compareTo(arg0.rating);
	}
	
	@Override
	public boolean equals(Object o){
		if(o instanceof Details){
			Details tp = (Details) o;
			return rating.equals(tp.rating) && businessID.equals(tp.businessID) && cateogry.equals(tp.cateogry) &&
					address.equals(tp.address);
		}
		return false;
	}
	
	@Override
	public int hashCode(){
		return businessID.hashCode() * 154 + address.hashCode() + cateogry.hashCode()+ rating.hashCode();
	}
	
}