package question4;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InMemoryReduceJoin {
	
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
			String[] inp = values.toString().split("\t");
			if (inp.length == 2) {
				context.write(new Text(inp[0]), new Text(inp[1]));
			}
		}
	}
	
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		private static HashMap<String, Integer> inMemMap = new HashMap<String, Integer>();
		private static final int THISYEAR = 2021;

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text friends : values) {
				String[] friendList = friends.toString().split(",");
				int count = 0;
				int max = 0;
				for (String s : friendList) {
					if (inMemMap.containsKey(s)) {
						count += 1;
						int age = inMemMap.get(s);
						if (age > max) {
							max = age;
						}
					}
				}
				if (count < 1) {
					context.write(key, new Text(""));  // no friend
				} else {
					context.write(key, new Text(Integer.toString(max)));
				}
			}
		}
		
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			Path part = new Path(conf.get("userdata"));
			FileSystem fileSystem = FileSystem.get(conf);
            FileStatus[] fileStatus = fileSystem.listStatus(part);
            for (FileStatus status : fileStatus) {
                Path pt = status.getPath();
                BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(pt)));
                String line;
                line = br.readLine();
                while (line != null) {
                    String[] arr = line.split(",");
                    String dob = arr[9];
                    String mdy[] = dob.split("/");
                    int year = Integer.parseInt(mdy[2]);
                    int age = THISYEAR - year;
                    inMemMap.put(arr[0], age);
                    line = br.readLine();
                }
            }
		}
	}

	
	
	

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		if (args.length != 3) {
			System.err.println("Wrong input! Try like: hadoop jar InMemoryReduceJoin.jar /input1 /input2 /output4");
			System.exit(1);
		}
		conf.set("userdata", args[1]);
		
		Job job = Job.getInstance(conf, "InMemoryReduceJoin");
		job.setJarByClass(InMemoryReduceJoin.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}


