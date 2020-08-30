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
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class AvgTempYearTwoReducers extends Configured implements Tool
{


	public static class AvgTempYearTwoReducersMapper extends Mapper<LongWritable, Text,SortYearKey , Pair>
	{

		private String year = new String();
	
		private Map<String,Pair> inMapperCombiner;
		
		public void setup(Context context){
			inMapperCombiner  = new HashMap<String,Pair>(); 
		}
			public void map(LongWritable key,Text value, Context context){
				
				year = (value.toString().substring(15,19));
				
				Pair temperature = new Pair(Integer.parseInt(value.toString().substring(87,92)),1);
			
				
				if(inMapperCombiner.containsKey(year)){
					Pair p=inMapperCombiner.get(year);
										
					inMapperCombiner.put(year, 
				new Pair(p.getTemp()+temperature.getTemp(),p.getCount()+1));
				}
				else
					inMapperCombiner.put(year.toString(), temperature);	
		}
			
			public void cleanup(Context context) throws IOException, InterruptedException {
				 
				for(String key: inMapperCombiner.keySet()){
					System.out.println(key.toString()+","+inMapperCombiner.get(key).getTemp()+","+inMapperCombiner.get(key).getCount());
					context.write(new SortYearKey(key), inMapperCombiner.get(key));
				}
			}
	}
	
	public static class AvgTempYearTwoReducersReducer extends Reducer<SortYearKey, Pair, Text, DoubleWritable>
	{
		
		@Override
		public void reduce(SortYearKey key, Iterable<Pair> values, Context context) throws IOException, InterruptedException
		{
			int count=0;
			double sum=0;
			for(Pair val:values){
				
				sum+=val.getTemp();
				count+=val.getCount();	
			}
			context.write(new Text(key.getYear()),new DoubleWritable(sum/(10*count)));			
		}
	}
	
	public static class  AveragTempYearPartitioner extends Partitioner<SortYearKey,Pair>{

		@Override
		public int getPartition(SortYearKey key, Pair pair, int numberReducerTask) {
			if(numberReducerTask == 0)
				return 0;
			
			if(Integer.parseInt(key.getYear()) < 1930)
				return 1%numberReducerTask ;
			else
			return 2%numberReducerTask;
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		
		FileSystem fileSystem = FileSystem.get(conf);
		if(fileSystem.exists(new Path(args[1]))){
		 
		   fileSystem.delete(new Path(args[1]),true);
		}
		
		int res = ToolRunner.run(conf, new AvgTempYearTwoReducers(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception
	{	
		
		Job job = new Job(getConf(), "AvgTempYearTwoReducers");
		job.setJarByClass(AvgTempYearTwoReducers.class);

		job.setMapperClass(AvgTempYearTwoReducersMapper.class);
		job.setReducerClass(AvgTempYearTwoReducersReducer.class);
 
		job.setMapOutputKeyClass(SortYearKey.class);
		job.setMapOutputValueClass(Pair.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputKeyClass(DoubleWritable.class);
		
		job.setNumReduceTasks(2);
		
		job.setPartitionerClass(AveragTempYearPartitioner.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}