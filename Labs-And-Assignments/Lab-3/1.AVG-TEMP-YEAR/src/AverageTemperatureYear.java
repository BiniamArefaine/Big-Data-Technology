import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class AverageTemperatureYear extends Configured implements Tool
{

	public static class AverageTempYearMapper extends Mapper<LongWritable, Text, Text, Text>
	{

		private Text year = new Text();
		private Text temp= new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
							
				year.set(value.toString().substring(15, 19));
				temp.set(value.toString().substring(87, 92));
				
				context.write(year, temp);				
			
		}
	}
	public static class AverageTempYearReducer extends Reducer<Text, Text, Text, DoubleWritable>
	{
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			int count=0;
			double sum=0;
			
			for(Text val:values){
				
				sum+=Double.parseDouble(val.toString())/10;
				count++;
				
			}		
			
			DoubleWritable average = new DoubleWritable((sum/(count)));
			context.write(key,average);	
			
		
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(args[1]))){
		 
		   fs.delete(new Path(args[1]),true);
		}
		
		int res = ToolRunner.run(conf, new AverageTemperatureYear(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception
	{	
		
		Job job = new Job(getConf(), "AverageTemp");
		job.setJarByClass(AverageTemperatureYear.class);

		job.setMapperClass(AverageTempYearMapper.class);
		job.setReducerClass(AverageTempYearReducer.class);
 
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(1);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}