import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class sort{
	public static class Map extends Mapper<LongWritable,Text,NullWritable,Text>{
		private TreeMap<Text, Text> student = new TreeMap<Text, Text>();
		public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException{
			String[] elements = value.toString().split(",");
			Text name = new Text(elements[0]);
			student.put(name, new Text(value));
		}
		protected void cleanup(Context context) throws IOException, InterruptedException{
			for(Text name : student.values()){
				context.write(NullWritable.get(),name);
			}
		}
	}
	
	public static class Reduce extends Reducer<NullWritable, Text, NullWritable, Text>{
		private TreeMap<Text, Text> student = new TreeMap<Text, Text>();
		public void reduce(NullWritable key, Text value, Context context) throws IOException,InterruptedException{
			String[] elements = value.toString().split(",");
			Text name = new Text(elements[0]);
			student.put(name, new Text(value));
		}
		protected void cleanup(Context context) throws IOException, InterruptedException{
			for(Text name : student.values()){
				context.write(NullWritable.get(),name);
			}
		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = new Job(conf,"sort");
		job.setJarByClass(sort.class);
		job.setMapperClass(Map.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(Reduce.class);
		//job.setNumReducers(1);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}