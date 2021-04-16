package question3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

public class InMemoryMapperJoin {

    public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
    	
        private HashMap<String, String> inMemMap = new HashMap<String, String>();
        private HashMap<String, Integer> countMap = new HashMap<String, Integer>();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String userA = context.getConfiguration().get("userA");
            String userB = context.getConfiguration().get("userB");
            StringBuilder tmp = new StringBuilder();

            String[] inp = value.toString().split("\t");
            if (inp.length == 2) {
            	String head = inp[0];

                if (head.equals(userA) || head.equals(userB)) {
                	String[] friendList = inp[1].split(",");
                	for (String name : friendList) {
                		if (countMap.containsKey(name)) {
                			int count = countMap.get(name);
                			if (count == 1) {
                				if (tmp.length() > 0) {
                					tmp.append(", ");
                				}
                				String detail = "Unknown";
                				if (inMemMap.containsKey(name)) {
                					detail = inMemMap.get(name);
                				}
                				tmp.append(detail);
                			}
                			countMap.replace(name, count + 1);
                		} else {
                			countMap.put(name, 1);
                		}
                	}
                }
            }
            
            if (tmp.length() > 0) {
            	context.write(new Text(userA + "," + userB), new Text("[" + tmp.toString() + "]"));
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
                    String fname = arr[1];
                    String lname = arr[2];
                    String dob = arr[9];
                    inMemMap.put(arr[0], fname + " " + lname + ":" + dob);
                    line = br.readLine();
                }
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        if (args.length != 5) {
            System.out.println("Wrong input! Try like: hadoop jar InMemoryMapperJoin.jar 6222 19272 /input1 /input2 /output3");
            System.exit(2);
        }

        conf.set("userA", args[0]);
        conf.set("userB", args[1]);
        conf.set("userdata", args[3]);

        
        Job job = new Job(conf, "InMemoryMapperJoin");
        job.setJarByClass(InMemoryMapperJoin.class);
        job.setMapperClass(MyMapper.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[4]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


