import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class counter{
	public enum ct {
		thirtycnt
	};
	public static class Map extends Mapper<LongWritable,Text,Text,IntWritable>{
		public void map(LongWritable key, Text value, Context context)
throws IOException, InterruptedException {
			String line = value.toString();
			String[] elements=line.split(",");
			int birthyear = Integer.parseInt(elements[3]);
			int age = 2018 - birthyear;
			Text name = new Text(elements[0]);
			if(age > 30){
				context.getCounter(ct.thirtycnt).increment(1);
				context.write(name,new IntWritable(birthyear));
			}
		}
	}
	public static void main(String[] args) throws Exception{
		Configuration conf=new Configuration();
		Job job = new Job(conf,"counter");
		job.setJarByClass(counter.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Map.class);
		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.waitForCompletion(true);
		Counters cn = job.getCounters();
		System.out.println(cn.findCounter(ct.thirtycnt).getValue());
	}
}