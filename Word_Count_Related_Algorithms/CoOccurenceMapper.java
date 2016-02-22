package WordCount.WordCount.StripesPackage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

public class CoOccurenceMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
	
	private static final Logger LOG = Logger.getLogger(CoOccurence.class);
    private MapWritable occurrenceMap = new MapWritable();
    private Text word = new Text();
    private boolean caseSensitive = false;
    private String input;
    private Set<String> stopWordSet = new HashSet<String>();
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

    protected void setup(Mapper.Context context)throws IOException,InterruptedException {
    	
    	Configuration conf = context.getConfiguration();	
    	int wordLength = Integer.parseInt(conf.get("Length"));
    	
      if (context.getInputSplit() instanceof FileSplit) {
        this.input = ((FileSplit) context.getInputSplit()).getPath().toString();
      } else {
        this.input = context.getInputSplit().toString();
      }
      Configuration config = context.getConfiguration();
      this.caseSensitive = config.getBoolean("wordcount.case.sensitive", false);
      if (config.getBoolean("wordcount.skip.patterns", false)) {
        URI[] localPaths = context.getCacheFiles();
        parseSkipFile(localPaths[0]);
      }
    }

    private void parseSkipFile(URI patternsURI) {
      LOG.info("Added file to the distributed cache: " + patternsURI);
      try {
        BufferedReader fis = new BufferedReader(new FileReader(new File(patternsURI.getPath()).getName()));
        String stopWord;
        while ((stopWord = fis.readLine()) != null) {
          stopWordSet.add(stopWord);
        }
      } catch (IOException ioe) {
        System.err.println("Caught exception while parsing the cached file '"
            + patternsURI + "' : " + StringUtils.stringifyException(ioe));
      }
    }

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
    	int wordLength=Integer.parseInt(context.getConfiguration().get("Length"));
    	List<String> wordTokenArr=new ArrayList<String>();
      String line = lineText.toString();
      if (!caseSensitive) {
        line = line.toLowerCase();
      }
      
      for (String word : WORD_BOUNDARY.split(line)) {
    	  if(word.length() == wordLength){
    		  if (word.isEmpty() || stopWordSet.contains(word)) {
    			  continue;
        }
        wordTokenArr.add(word);
    	  }
      }
      
      int neighbors=2;
      String[] tokens=new String[wordTokenArr.size()];
      for (int i=0; i<wordTokenArr.size();i++) {
    	  tokens[i]= wordTokenArr.get(i);
      }
	  
      if (tokens.length > 1) {
	    for (int i = 0; i < tokens.length; i++) {
	        word.set(tokens[i]);
	        occurrenceMap.clear();
	
	        int start = (i - neighbors < 0) ? 0 : i - neighbors;
	        int end = (i + neighbors >= tokens.length) ? tokens.length - 1 : i + neighbors;
	         for (int j = start; j <= end; j++) {
	              if (j == i) continue;
	              Text neighbor = new Text(tokens[j]);
	              if(occurrenceMap.containsKey(neighbor)){
	                 IntWritable count = (IntWritable)occurrenceMap.get(neighbor);
	                 count.set(count.get()+1);
	              }else{
	                 occurrenceMap.put(neighbor,new IntWritable(1));
	              }
	         }
	        context.write(word,occurrenceMap);
	     }
      }             
  }
}

