package WordCount.WordCount.StripesPackage;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class CoOccurenceReduce extends Reducer<Text, MapWritable, Text, Text> {
	  private MapWritable incrementingMap = new MapWritable();
	  @Override
	    protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
	        incrementingMap.clear();
	        for (MapWritable value : values) {
	            addAll(value);
	            
	        }
	        StringBuilder sb= new StringBuilder("{");
	        for (java.util.Map.Entry<Writable, Writable> entry : incrementingMap.entrySet()){
	        	if(sb.toString().length()!=1)
	        	{
	        		sb.append(",");
	        	}
	        	sb.append(entry.getKey()+":"+entry.getValue());
	        	}
	        sb.append(",");
	        context.write(key, new Text(sb.toString()));
	    }

	    private void addAll(MapWritable mapWritable) {
	        Set<Writable> keys = mapWritable.keySet();
	        for (Writable key : keys) {
	            IntWritable fromCount = (IntWritable) mapWritable.get(key);
	            if (incrementingMap.containsKey(key)) {
	                IntWritable count = (IntWritable) incrementingMap.get(key);
	                count.set(count.get() + fromCount.get());
	            } else {
	                incrementingMap.put(key, fromCount);
	            }
	        }
	    }
}