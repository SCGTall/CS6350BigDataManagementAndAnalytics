package question1;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MutualFriends {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
			String[] inp = values.toString().split("\t");
			if (inp.length == 2) {
				int id1 = Integer.parseInt(inp[0]);
				String friendsStr = inp[1];
				String[] friendList = friendsStr.split(",");
				for (String name : friendList) {
					int id2 = Integer.parseInt(name);
					Text imKey = new Text();
					if (id1 < id2) {
						imKey.set(id1 + "," + id2);
					} else {
						imKey.set(id2 + "," + id1);
					}
					context.write(imKey, new Text(friendsStr));

				}
			}
		}
	}


	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		
		// only consider target pairs
		private static final List<String> targets = Arrays.asList("0,1",
															"20,28193",
		                                                    "1,29826",
		                                                    "6222,19272",
		                                                    "28041,28056");
		private static boolean useTargets = true;

		public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			if (useTargets && !targetsContainKey(key)) {  // skip pairs which are not interested
				return;
			}
			HashMap<String, Integer> map = new HashMap<>();
			StringBuilder tmp = new StringBuilder();
			for (Text friends : values) {
				String[] friendList = friends.toString().split(",");
				for (String s : friendList) {
					if (map.containsKey(s)) {
						int count = map.get(s);
						if (count == 1) {  // only output when s appears second time.
							if (tmp.length() <= 0) {
								tmp.append(s);
							} else {
								tmp.append(',');
								tmp.append(s);
							}
						}
						map.replace(s, count + 1);  // ++
						
					} else {
						map.put(s, 1);
					}
				}
			}
			String commonFriendsStr = tmp.toString();
			context.write(key, new Text(commonFriendsStr));
		}
		
		private static boolean targetsContainKey(Text key) {
			String tmp = key.toString();
			for (String s : targets) {
				if (s.equals(tmp)) {
					return true;
				}
			}
			return false;
		}
		
	}

    public static void main(String[] args) throws Exception{
		if(args.length != 2) {
			System.out.println("Wrong input! Try like: hadoop jar MutualFriends.jar /input1 /output1");
			System.exit(1);
		}

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "MutualFriends");

		job.setJarByClass(MutualFriends.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}



