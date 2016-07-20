/*
 *  @Gaurav
 */
import java.util.*;
import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;


public class Driver {
	
	public static int isit(String word){
		
		int flag=0;
		int length=word.length();
		if(length==1){
			return 0;
		}
		
		if(word.startsWith("&") || word.startsWith("-")){
			return 0;
			
		}
		else if(word.charAt(0)==word.charAt(length-1)){
			flag=1;
			
		}
		
		return flag;
	}
	
	public static char startWith(String word){
		
		
		char l = word.charAt(0);
		return l;
			
	}
	
	//---------------------------Mapper class-------------------------------------------
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>{
		Text word1 = new Text();
		Text word2 = new Text();
		public void map(LongWritable k,Text value,Context context)
		  throws IOException, InterruptedException{
			
			String line = value.toString();
			StringTokenizer str = new StringTokenizer(line);
			while(str.hasMoreTokens()){
				String kk=str.nextToken();
				Character ch;
				int num = Driver.isit(kk);
				if(num==1){
				  word1.set(kk);
				  ch = Driver.startWith(kk);
				  word2.set(ch.toString());
				  
				}
				context.write(word1, word2);
			}
		}
		
	}
	
	//-----------------------------Driver Class----------------------------------------

	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		
		job.setJarByClass(Driver.class);
		
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	    job.setMapperClass(MapClass.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    
	    System.exit(job.waitForCompletion(true)?1:0);
	}
	
}
