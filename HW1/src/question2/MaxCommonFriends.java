package question2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;


public class MaxCommonFriends {
	
	public static class MyMapper extends Mapper<Text, Text, LongWritable, Text> {

		private LongWritable count = new LongWritable();

		public void map(Text key, Text values, Context context) throws IOException, InterruptedException {
			String[] friendList = values.toString().split(",");
			int newcount = friendList.length;
			count.set(newcount);
			context.write(count, key);
		}
	}

	public static class MyReducer extends Reducer<LongWritable, Text, Text, LongWritable> {
		private int index = 0;

		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Text max = new Text();
			for (Text value : values) {
				if (index < 1) {
					max = value;
					index++;
				}
				if (max.equals(value)) {
					context.write(value, key);
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			System.out.println("Wrong input! Try like: hadoop jar MaxCommonFriends.jar /output1 /output2");
			System.exit(1);
		}

		{
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "MaxCommonFriends");

			job.setJarByClass(MaxCommonFriends.class);
			job.setMapperClass(MyMapper.class);
			job.setReducerClass(MyReducer.class);

			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(Text.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);

			job.setInputFormatClass(KeyValueTextInputFormat.class);

			job.setSortComparatorClass(LongWritable.DecreasingComparator.class);

			job.setNumReduceTasks(1);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	}
}


