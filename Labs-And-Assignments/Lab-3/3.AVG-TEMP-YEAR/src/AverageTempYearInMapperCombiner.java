import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
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


public class AverageTempYearInMapperCombiner extends Configured implements Tool
{


	public static class AverageTempYearMapper extends Mapper<LongWritable, Text, Text, Pair>
	{

		private String year = new String();
	
		private Map<String,Pair> inMapperCombiner;
		
		public void setup(Context context){
			inMapperCombiner  = new HashMap<String,Pair>(); 
		}
			public void map(LongWritable key,Text value, Context context){
				
				year = (value.toString().substring(15,19));
				
				Pair temp = new Pair(Integer.parseInt(value.toString().substring(87,92)),1);
			
				
				if(inMapperCombiner.containsKey(year)){
					Pair pair=inMapperCombiner.get(year);
										
					inMapperCombiner.put(year, 
				new Pair(pair.getTemp()+temp.getTemp(),pair.getCount()+1));
				}
				else
					inMapperCombiner.put(year.toString(), temp);	
		}
			
			public void cleanup(Context context) throws IOException, InterruptedException {
				 
			 	
				 
				for(String key: inMapperCombiner.keySet()){
					System.out.println(key.toString()+","+inMapperCombiner.get(key).getTemp()+","+inMapperCombiner.get(key).getCount());
					context.write(new Text(key), inMapperCombiner.get(key));
				}
			}
	}
	
	public static class AverageTempYearReducer extends Reducer<Text, Pair, Text, DoubleWritable>
	{
		
		@Override
		public void reduce(Text key, Iterable<Pair> values, Context context) throws IOException, InterruptedException
		{
			int count=0;
			double sum=0;
			for(Pair val:values){
				
				sum+=val.getTemp();
				count+=val.getCount();	
			}
			context.write(key,new DoubleWritable(sum/(10*count)));			
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		
		FileSystem fileSystem = FileSystem.get(conf);
		if(fileSystem.exists(new Path(args[1]))){
		 
			fileSystem.delete(new Path(args[1]),true);
		}
		
		int res = ToolRunner.run(conf, new AverageTempYearInMapperCombiner(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception
	{	
		
		Job job = new Job(getConf(), "AverageTempYearInMapperCombiner");
		job.setJarByClass(AverageTempYearInMapperCombiner.class);

		job.setMapperClass(AverageTempYearMapper.class);
		job.setReducerClass(AverageTempYearReducer.class);
 
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Pair.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputKeyClass(DoubleWritable.class);
		
		job.setNumReduceTasks(1);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}