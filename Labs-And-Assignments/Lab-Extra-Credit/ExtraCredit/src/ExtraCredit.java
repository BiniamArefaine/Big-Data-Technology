

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

public class ExtraCredit extends Configured implements Tool {

	public static class ExtraCreditMapper extends Mapper<LongWritable, Text, ObjectKeys, Text> {

		private Text year = new Text();
		private Text temp = new Text();
		private Text stationId = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			year.set(value.toString().substring(15, 19));
			
			temp.set(value.toString().substring(87, 92));
		
			stationId.set(value.toString().substring(4, 10) + "-" + value.toString().substring(10, 15));

			context.write(new ObjectKeys(stationId.toString() + "," + temp.toString()), year);

		}
	}

	public static class ExtraCreditReducer extends Reducer<ObjectKeys, Text, Text, Text> {

		@Override
		public void reduce(ObjectKeys key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for (Text year : values) {

				context.write(new Text(key.getStationId_temp().replace(",", "\t")), year);

			}

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		FileSystem fileSystem = FileSystem.get(conf);
		if (fileSystem.exists(new Path(args[1]))) {

			fileSystem.delete(new Path(args[1]), true);
		}

		int res = ToolRunner.run(conf, new ExtraCredit(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf(), "ExtraCredit");
		job.setJarByClass(ExtraCredit.class);

		job.setMapperClass(ExtraCreditMapper.class);
		job.setReducerClass(ExtraCreditReducer.class);

		job.setMapOutputKeyClass(ObjectKeys.class);
		job.setMapOutputValueClass(Text.class);

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