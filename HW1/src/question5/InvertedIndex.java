package question5;

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

public class InvertedIndex {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		private static int lineNum = 1;

		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
			String[] inp = values.toString().split("[,｜ ]"); // regular expression
			for (String word : inp) {
				String relaxed = word.trim();
				if (relaxed.length() > 0 && relaxed.charAt(0) == '(') {
					relaxed = relaxed.substring(1);
				}
				if (relaxed.length() > 0 && relaxed.charAt(relaxed.length() - 1) == ')') {
					relaxed = relaxed.substring(0, relaxed.length() - 1);
				}
				if (relaxed.length() > 0) {
					context.write(new Text(relaxed), new Text(Integer.toString(lineNum)));
				}
			}
			lineNum += 1;
		}
	}


	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		
		public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			StringBuilder tmp = new StringBuilder();
			for (Text lineNum : values) {
				if (tmp.length() > 0) {
					tmp.append(", ");
				}
				tmp.append(lineNum.toString());
			}
			String listStr = "[" + tmp.toString() + "]";
			context.write(key, new Text(listStr));
		}
		
	}

    public static void main(String[] args) throws Exception{
		if(args.length != 2) {
			System.out.println("Wrong input! Try like: hadoop jar InvertedIndex.jar /input2 /output5");
			System.exit(1);
		}

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "InvertedIndex");

		job.setJarByClass(InvertedIndex.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}



