
import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
public class countnew{
	public enum ct {
		cnt,nt
	};
	public static class Map extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] elements=line.split(",");
			int i = Integer.parseInt(elements[1]);
			float exp = Float.parseFloat(elements[0]);
			Text tt = new Text(elements[3]);
			if(exp == 0.0)
			{
				context.getCounter(ct.cnt).increment(1);
				context.write(tt,new IntWritable(i));
			}
			if(i > 50000)
			{
				context.getCounter(ct.nt).increment(1);
			}
		}
	}
	public static void main(String[] args) throws Exception
	{
		Configuration conf=new Configuration();
		Job job = new Job(conf,"countnew");
		job.setJarByClass(countnew.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(Map.class);
		job.setNumReduceTasks(0);
			// job.setInputFormatClass(TextInputFormat.class);
			// job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.waitForCompletion(true);
		Counters cn=job.getCounters();
		cn.findCounter(ct.cnt).getValue();
		cn.findCounter(ct.nt).getValue();
	}
}

