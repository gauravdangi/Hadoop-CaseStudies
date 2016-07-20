package vowels;
import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Vowels {
	
	public static class MapClass extends Mapper <LongWritable, Text,Text,IntWritable>{
		
		Text vow = new Text();
		
		public void map(LongWritable k, Text t, Context context)
		 throws IOException,InterruptedException{
			
			String word = t.toString();
			StringTokenizer str = new StringTokenizer(word);
			while(str.hasMoreTokens()){
				String a = str.nextToken();
				if(a.startsWith("a") || a.startsWith("A")){
					vow.set("a");}
				else if(a.startsWith("e") || a.startsWith("E")){
					vow.set("e");}
				else if(a.startsWith("i") || a.startsWith("I")){
					vow.set("i");}
				else if(a.startsWith("o") || a.startsWith("O")){
					vow.set("o");}
				else if(a.startsWith("u") || a.startsWith("U")){
					vow.set("u");}
				context.write(vow, new IntWritable(1));
					
				}
			
				
			}
			
		}
	
	//---------------------------Reducer class----------------------------------------------------------
	
	public static class ReduceClass extends Reducer<Text, IntWritable,Text,IntWritable>{
		
		public void reduce(Text word, Iterable<IntWritable> t, Context context)
		  throws IOException, InterruptedException{
			
			int count = 0;
			for(IntWritable i:t){
				
				count+=i.get();
			}
			
			context.write(word, new IntWritable(count));
			
		}
		
	}
	
	//------------------------------------Main---------------------------------------------
	
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(Vowels.class);
		job.setJobName("Counting no. of words with each vowel");
		
		Path Outputpath = new Path(args[1]); 
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		Outputpath.getFileSystem(conf).delete(Outputpath);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
		
	}


