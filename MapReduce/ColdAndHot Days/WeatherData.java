import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class WeatherData {

	public static class MaxTemperatureMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable arg0, Text Value,
				OutputCollector<Text, Text> output, Reporter arg3)
				throws IOException {

			String line = Value.toString();

			  // Example of Input 
			  //         Date                              Max     Min 
			  // 25380 20130101  2.514 -135.69   58.43     8.3     1.1     4.7     4.9     5.6     0.01 C     1.0    -0.1     0.4    97.3    36.0    69.4 -99.000 -99.000 -99.000 -99.000 -99.000 -9999.0 -9999.0 -9999.0 -9999.0 -9999.0
			
			
			String date = line.substring(6, 14);

			float temp_Max = Float.parseFloat(line.substring(39, 45).trim());
			float temp_Min = Float.parseFloat(line.substring(47, 53).trim());

			if (temp_Max > 40.0) {
				// Hot day
				output.collect(new Text("Hot Day " + date),
						new Text(String.valueOf(temp_Max)));
			}

			if (temp_Min < 10) {
				// Cold day
				output.collect(new Text("Cold Day " + date),
						new Text(String.valueOf(temp_Min)));
			}
		}

	}

	public static class MaxTemperatureReducer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text Key, Iterator<Text> Values,
				OutputCollector<Text, Text> output, Reporter arg3)
				throws IOException {

			// Find Max temp yourself ?
			String temperature = Values.next().toString();
			output.collect(Key, new Text(temperature));
		}

	}

	public static void main(String[] args) throws Exception {

		JobConf conf = new JobConf(WeatherData.class);
		conf.setJobName("temp");

		// Note:- As Mapper's output types are not default so we have to define
		// the
		// following properties.
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setMapperClass(MaxTemperatureMapper.class);
		conf.setReducerClass(MaxTemperatureReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);

	}
}
