
import java.io.IOException; 
import java.util.StringTokenizer; 
 
/*
 * All org.apache.hadoop packages can be imported using the jar present in lib 
 * directory of this java project.
 */



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

 

/**
* <p>The Temperature program finds out the maximum temperature in every year from the given input file. 
* The output data is in the form of “Year and Maximum temperature corresponding to that year".
* We write a map reduce code to achieve this, where mapper makes key value pair from the input file
* and reducer does aggregation on this key value pair. 
*/

public class Max_temp { 
	
	/** 
	 * @interface Mapper
	 * <p>Map class is static and extends MapReduceBase and implements Mapper 
	 * interface having four hadoop generics type LongWritable, Text, Text, 
	 * IntWritable.
	 */
 
	public static class Map extends
	Mapper<LongWritable, Text, Text, IntWritable> {
    	//Mapper
		
		
    	/**
    	 * @method map
    	 * <p>This method takes the input as text data type and and tokenizes input
    	 * by taking whitespace as delimiter. The first token goes year and second token is temperature,
    	 * this is repeated till last token. Now key value pair is made and passed to reducer.                                             
    	 * @method_arguments key, value, output, reporter
    	 * @return void
    	 */	
    	
    	//Defining a local variable k of type Text  
    	Text k= new Text(); 

    	/*
    	 * (non-Javadoc)
    	 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
    	 */
    	
        @Override 
        public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {

        		//Converting the record (single line) to String and storing it in a String variable line
                String line = value.toString(); 

                //StringTokenizer is breaking the record (line) according to the delimiter whitespace
                StringTokenizer tokenizer = new StringTokenizer(line," "); 
 
                //Iterating through all the tokens and forming the key value pair
                while (tokenizer.hasMoreTokens()) { 

                //The first token is going in year variable of type string
            	String year= tokenizer.nextToken();
            	k.set(year);

            	//Takes next token and removes all the whitespaces around it and then stores it in the string variable called temp
            	String temp= tokenizer.nextToken().trim();

            	//Converts string temp into integer v       	
            	int v = Integer.parseInt(temp); 

            	//Sending to output collector which inturn passes the same to reducer
                context.write(k,new IntWritable(v)); 
            } 
        } 
    } 
 
    //Reducer
	
    /** 
        * @interface Reducer
     * <p>Reduce class is static and extends MapReduceBase and implements Reducer 
     * interface having four hadoop generics type Text, IntWritable, Text, IntWritable.
     */
    
	public static class Reduce extends
	Reducer<Text, IntWritable, Text, IntWritable> {

    	/**
    	 * @method reduce
    	 * <p>This method takes the input as key and list of values pair from mapper, it does aggregation
    	 * based on keys and produces the final output.                                               
    	 * @method_arguments key, values, output, reporter	
    	 * @return void
    	 */	
    	     	 

    	 /*
    	  * (non-Javadoc)
    	  * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object, java.util.Iterator, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
    	  */
    	 
        @Override 
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException {
        	/*
        	 * Iterates through all the values available with a key and if the integer variable temperature
        	 * is greater than maxtemp, then it becomes the maxtemp
        	 */

          //Defining a local variable maxtemp of type int
        	int maxtemp=0;
            for(IntWritable it : values) { 

            //Defining a local variable temperature of type int which is taking all the temperature
            int temperature= it.get();
            	if(maxtemp<temperature)
            	{
            		maxtemp =temperature;
            	}
            } 
            
            //Finally the output is collected as the year and maximum temperature corresponding to that year
            context.write(key, new IntWritable(maxtemp)); 
        } 
 
    } 
    
  //Driver
	

    /**
     * @method main
     * <p>This method is used for setting all the configuration properties.
     * It acts as a driver for map reduce code.
     * @return void
     * @method_arguments args
     * @throws Exception
     */
 
    public static void main(String[] args) throws Exception { 
 
    	//reads the default configuration of cluster from the configuration xml files
		
		Configuration conf = new Configuration();
		
		//Initializing the job with the default configuration of the cluster
		
		
		Job job = new Job(conf, "Max_temp");
		
		//Assigning the driver class name 
		
		job.setJarByClass(Max_temp.class);
		
		//Defining the mapper class name
		
		job.setMapperClass(Map.class);
		
		//Defining the reducer class name
		
		job.setReducerClass(Reduce.class);
		
		//Defining the output key class for the final output i.e. from reducer
		
		job.setOutputKeyClass(Text.class);
		
		//Defining the output value class for the final output i.e. from reducer
		
		job.setOutputValueClass(IntWritable.class);
		
		//Defining input Format class which is responsible to parse the dataset into a key value pair 
		
		job.setInputFormatClass(TextInputFormat.class);
		
		//Defining output Format class which is responsible to parse the final key-value output from MR framework to a text file into the hard disk
		
		job.setOutputFormatClass(TextOutputFormat.class);
 
        //setting the second argument as a path in a path variable
		
        Path outputPath = new Path(args[1]);
		
        //Configuring the input/output path from the filesystem into the job
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//deleting the output path automatically from hdfs so that we don't have delete it explicitly
		
		outputPath.getFileSystem(conf).delete(outputPath);
		
		//exiting the job only if the flag value becomes false
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
							
 
    } 
}
