
import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
public class partition{
	public static class Map extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException
		{
		
			String[] line=value.toString().split(",");
			int i=Integer.parseInt(line[1]);
			context.write(new Text(line[3]), new IntWritable(i));
		}
	}
	public static class dpart extends Partitioner<Text,IntWritable>
	{
		public int getPartition(Text key,IntWritable value,int nr)
		{
			if(value.get()<30000)
				return 0;
			if(value.get() < 50000)
				return 1;
			else
				return 2;
		}
	}
	public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>
	{
		public void reduce(Text  key,IntWritable value,Context context)throws IOException,InterruptedException
		{
	
			context.write(key,value);
		}
		
	}
	public static void main(String[] args) throws Exception
	{
		Configuration conf=new Configuration();
		Job job=new Job(conf,"partition");
		job.setJarByClass(partition.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Map.class);
		job.setPartitionerClass(dpart.class);
		job.setNumReduceTasks(3);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.waitForCompletion(true);

	}

}

