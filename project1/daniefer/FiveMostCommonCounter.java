package project1.daniefer;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;


public class FiveMostCommonCounter extends Configured implements Tool{
	
	public static class MyMap extends Mapper<LongWritable, Text, Text, IntWritable> 
	{
		public static String Searchword = null;
		private final static IntWritable one = new IntWritable(1);
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException
		{
			if (Searchword == null)
			{
				Searchword = context.getConfiguration().get("keyword").toString();
			}
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException
		{	
			// Input key/values are file chunks by default so they will need to be split up.
			String[] lines = value.toString().split("\\r?\\n"); /* Split input block on new line*/
			for (String line : lines) /*For each line, search article.*/
			{
				if (line == null || line.trim().equals("")) {continue;}
				String[] elements = line.split("\\t"); /*Split line over tabs*/
				if (elements[1] != null && elements[1].contains(Searchword)) /*If the title contains the Searchword, process article*/
				{
					String[] title = elements[1].split("[\\W]");
					for (int i = 0; i < title.length; i++) {
						//If not blank save word
						if (!title[i].equals("")){
							context.write(new Text(title[i]), one);
						}
					}

					if (elements[3] == null || elements[3].trim().equals("")) {continue;}
					
					String[] content = elements[3].split("[\\W]");
                                        for (int i = 0; i < content.length; i++) {
                                                //if not blank save word
						if (!content[i].equals("")){
							context.write(new Text(content[i]), one);
                                        	}
					}
				}
			}
		}
	}
	
	public static class MyCombine extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException
		{
			int total = 0;
			Iterator<IntWritable> vals = values.iterator();
			while (vals.hasNext()) {
				vals.next();
				total++;
			}
			IntWritable finalCount = new IntWritable(total);
			context.write(key, finalCount);
		}
	}
	
	public static class MyReduce extends Reducer<Text, IntWritable, Text, IntWritable> 
	{
		private java.util.Map<String, Integer> dic = new HashMap<String, Integer>();
		
		 @Override
		 public void reduce(Text key, Iterable<IntWritable> values, Context context) 
				 throws IOException, InterruptedException
		 {
			 //TODO: add all occurrences of the key
			 int sum = 0;
			 Iterator<IntWritable> vals = values.iterator();
			 while (vals.hasNext()) 
			 {
				 sum += vals.next().get();
			 }
			 dic.put(key.toString(), sum);
			 context.write(key, new IntWritable(sum));
		 }
		
		@Override 
		public void cleanup(Context context) 
				 throws IOException, InterruptedException
		{
			 // Sorting the dictionary largest to smallest
			 List<FiveMostCommonCounter.MyReduce.KeyValuePair> pairs = new LinkedList<FiveMostCommonCounter.MyReduce.KeyValuePair>();
			 
			 for (Entry<String, Integer> entry : dic.entrySet()) {
				pairs.add(new KeyValuePair(entry.getKey(), entry.getValue()));
			 }
			 
			 Collections.sort(pairs, new Comparator<KeyValuePair>() {
			     public int compare(KeyValuePair e1, KeyValuePair e2) {
			    	 return e2.Value.compareTo(e1.Value);
			     }
			 });
			
			 //Print out the 4 most common words
			 if (pairs.size() > 4)
			 {
			 context.write(new Text("1st " + pairs.get(0).Key), new IntWritable(pairs.get(0).Value));
			 context.write(new Text("2nd " + pairs.get(1).Key), new IntWritable(pairs.get(1).Value));
			 context.write(new Text("3rd " + pairs.get(2).Key), new IntWritable(pairs.get(2).Value));
			 context.write(new Text("4th " + pairs.get(3).Key), new IntWritable(pairs.get(3).Value));
			 context.write(new Text("5th " + pairs.get(4).Key), new IntWritable(pairs.get(4).Value));
			 }
			 else if (dic.size() > 4)
			 {
				context.write(new Text("dic.size()"), new IntWritable(dic.size()));
			 }
			 else
			 {
				context.write(new Text("Failed"), new IntWritable(0));
			 }
		}
		
		public class KeyValuePair
		{
			 public String Key;
			 public Integer Value;
			 
			 public KeyValuePair(String key, Integer value)
			 {
				 Key = key;
				 Value = value;
			 }
		}
	}
	
	public int run(String[] args) 
			throws Exception 
	{
		Configuration config = this.getConf();
		
		String outputPath = null;
		List<String> other_args = new ArrayList<String>();
	    	for (int i=0; i < args.length; ++i) 
	    	{
	    		if ("-parameters".equals(args[i].toString())) /*Get parameters and pass to config for Map class*/
		    	{
		    		for (String pair : args[++i].split(";")) /*split parameters string on ';'.*/
	    			{
	    				String[] keyvalue = pair.split("="); /*Split key/value pair on '='.*/
		    			config.set(keyvalue[0].toString(), keyvalue[1] == null ? "" :  keyvalue[1].toString());
					}	
		    	}
	    		else if ("-o".equals(args[i].toString())) /*Get output path if specified*/
	    		{
	    			outputPath = args[++i].toString();
	    		}
	    		else
	    		{
	    			other_args.add(args[i]); /*Save none special args*/
	    		}
	   	 }
	    	
		// Make sure a keyword was provided
	    	if (config.get("keyword") == null || config.get("keyword") == "")
	    	{
			if (other_args.size() < 2)
			{
				System.out.println("Parameter 'keyword' was not specified.");
				System.exit(1);
			}
			config.set("keyword", other_args.get(1));
	    	}
		
		//Notify user what key word will be searched for
		System.out.println("Searching for keyword: " + config.get("keyword"));
	    	
		if (outputPath == null)
	    	{
	    		outputPath = System.getenv("MY_HADOOP_HOME"); /*If output path is not specified, make it the users home directory*/
			outputPath += "/FiveMostCommonCounter";
	    	}
	    	System.out.println("Output will be saved to: " + outputPath);
		Job job = Job.getInstance(config); /*Creates a new job instance not associated with a Cluster*/

	  	job.setOutputKeyClass(Text.class); /*Sets output Key class*/
	    	job.setOutputValueClass(IntWritable.class); /*Sets output Value class*/

		job.setJobName("FiveMostCommonCounter");
		job.setJarByClass(FiveMostCommonCounter.class);
		job.setMapperClass(MyMap.class); /*Sets Map class*/
		job.setCombinerClass(MyCombine.class); /*Sets Combiner class*/
		job.setReducerClass(MyReduce.class); /*Sets Reduce class*/
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(1);

	    	job.setInputFormatClass(TextInputFormat.class); /*Sets InputFormat class*/
	    	job.setOutputFormatClass(TextOutputFormat.class); /*Sets OutputFormat class*/
	    
	    	FileInputFormat.setInputPaths(job, new Path(other_args.get(0))); /*Sets input path*/
	    	FileOutputFormat.setOutputPath(job, new Path(outputPath)); /*Sets output path*/

	    	return job.waitForCompletion(true) ? 0 : 1; /*Starts job and waits for completion*/
	}
}
