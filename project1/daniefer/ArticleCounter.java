package project1.daniefer;

import java.io.IOException;
import java.util.*;

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


public class ArticleCounter extends Configured implements Tool 
{
	
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
				//Loads keyword from the config object
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
				if (line == null || line.trim().equals("")) {continue;} //Skip over bank or null lines
				String[] elements = line.split("\\t"); /*Split line over tabs*/
				
				if (elements[1].contains(Searchword) || elements[3].contains(Searchword))
				{
					// If the keyword is found in the string write the output the articleID and 1
					context.write(new Text(elements[0].toString()), one); 
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
			// consolidate key/value pairs to reduce network traffic
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
		public static int total;
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
				 throws IOException, InterruptedException
		{
			 int sum = 0;
			 Iterator<IntWritable> vals = values.iterator();
			 while (vals.hasNext()) 
			 {
				 // consolidate key/value pairs (should only look through once, but it makes the code more dynamic)
				 sum += vals.next().get();
			 }
			 total += sum; //Save total article count

			 context.write(key, new IntWritable(sum)); // write the article ids and article count to the output (this should alwyas be one since article keys are unique)
		}

		@Override
		public void cleanup(Context context)
				throws IOException, InterruptedException
		{
			context.write(new Text("Total Count"), new IntWritable(total)); // Write total count as last line. This method is only called once, after every reducer on this node is complete. 
		}
	}
	
	@Override
	public int run(String[] args) 
			throws Exception 
	{
		Configuration config = this.getConf();
		
		//Parse command line arguments
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
	    	
		//Error checking
	   	if (config.get("keyword") == null || config.get("keyword") == "")
	    	{
			if (other_args.size() < 2)
                	{
                        	System.out.println("Parameter 'keyword' was not specified.");
                        	System.exit(1);
                	}
                	config.set("keyword", other_args.get(1));
	    	}
		
		//Info output to user
		System.out.println("Searching for keyword: " + config.get("keyword"));

		// Set output path if not specified
                if (outputPath == null)
                {
                        outputPath = System.getenv("MY_HADOOP_HOME"); /*If output path is not specified, make it the users home directory*/
                        outputPath += "/ArticleCounter";
                }

		//Info output to user
                System.out.println("Output will be saved to: " + outputPath);

		Job job = Job.getInstance(config); /*Creates a new job instance not associated with a Cluster*/
		
	    	job.setOutputKeyClass(Text.class); /*Sets job output Key class*/
	    	job.setOutputValueClass(IntWritable.class); /*Sets job output Value class*/
		
		job.setJobName("ArticleCounter"); /*Sets job name*/
                job.setJarByClass(ArticleCounter.class); /*Sets the class name in the .jar*/
	    	job.setMapperClass(MyMap.class); /*Sets Map class*/
	    	job.setCombinerClass(MyCombine.class); /*Sets Combiner class*/
	    	job.setReducerClass(MyReduce.class); /*Sets Reduce class*/

	    	job.setMapOutputKeyClass(Text.class);
	    	job.setMapOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class); /*Sets InputFormat class*/
	    	job.setOutputFormatClass(TextOutputFormat.class); /*Sets OutputFormat class*/
	    	
		job.setNumReduceTasks(1);
		
	    	FileInputFormat.setInputPaths(job, new Path(other_args.get(0))); /*Sets input path*/
	    	FileOutputFormat.setOutputPath(job, new Path(outputPath)); /*Sets output path*/

	    	return job.waitForCompletion(true) ? 0 : 1; /*Starts job and waits for completion*/
    }
}
