package project1;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import project1.daniefer.ArticleCounter;
import project1.daniefer.FiveMostCommonCounter;

public class Main {

	public static void main(String[] args) 
			 throws Exception 
	 {
		String className = "";
		Tool tool = null;
		List<String> other_args = new ArrayList<String>();
		for (int i=0; i < args.length; ++i) 
		{
			if ("-program".equals(args[i])) /*Get program type*/
			{
				className = args[++i];
			}
			else if (!args[i].startsWith("-"))
			{
				other_args.add(args[i]);
			}
		}
		
		if (other_args.size() < 2)
		{
			boolean repeat = true;
			String keyword = null;
			while (repeat)
			{
				System.out.print("Enter a parameter string: ");
				 
			    //  open up standard input
			    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			      
			    try {
			    	  keyword = br.readLine();
				} catch (IOException e) {
					repeat = false;
					System.exit(1);
				}
			    
			    if (keyword == null || keyword.equals("")) 
			    {
			    	System.out.print("Searchword cannot be blank. Try again. Ctrl+C to cancel.");
			    	repeat = true;
			    }
			    else
			    {
				other_args.add("-parameters");
				other_args.add(keyword);
				repeat = false;
			    }	
			} 
		}
		
		if (className == null || className.equals(""))
		{
			boolean repeat = true;
			while (repeat)
			{
				System.out.print("Enter a class name. ArticleCounter (1), FiveMostCommonCounter (2): ");
				 
			    //  open up standard input
			    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			      
			    try {
			    	  className = br.readLine();
				} catch (IOException e) {
					repeat = false;
					System.exit(1);
				}
			    
			    if (className.equals("1")) {
					className = "ARTICLECOUNTER";
					repeat = false;
				}
			    else if (className.equals("2"))
				{
					className = "FIVEMOSTCOMMONCOUNTER";
					repeat = false;
				}
			    else
			    {
			    	System.out.print(className + " is not a valid option. Try again. Ctrl+C to cancel.");
			    	repeat = true;
			    }
			}
		}	    
		if (className.toUpperCase().equals("FIVEMOSTCOMMONCOUNTER")) 
		{
	    		tool = new FiveMostCommonCounter();
		}
		else if (className.toUpperCase().equals("ARTICLECOUNTER"))
		{
 			tool = new ArticleCounter();
	    	}
	    	else
	    	{
	    		System.out.printf("The class '%1s' does not exists. Available classes are 'FiveMostCommonCounter' and 'ArticleCounter'.",args[2].toString());
	    	}
		
		System.out.println("Running " + className + ".");
		
		if (tool != null)
		{
			String[] inputArgs = new String[other_args.size()];
			for (int j = 0; j < inputArgs.length; j++) {
				inputArgs[j] = other_args.get(j);
			}
			int ret = ToolRunner.run(new Configuration(), tool, inputArgs);
			System.exit(ret);
		}
		else
		{
			return;
		}
	 }
}
