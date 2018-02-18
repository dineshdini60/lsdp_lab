
import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class nlinetool extends Configured implements Tool{
	public static class Map extends Mapper<Object,Text,Text,Text>
	{
		String c = "ram";
		public void map(Text key,Text value,Context context)throws IOException,InterruptedException
		{
			String line=key.toString();
			if(c.equalsIgnoreCase(line))
				context.write(key,value);
		}
	}
	public int run(String[] args) throws Exception{
		Job job = new Job(getConf());
		job.setJobName("NLineInputFormat example");
		job.setJarByClass(nlinetool.class);
		job.setMapperClass(Map.class);
		job.setInputFormatClass(NLineInputFormat.class);
		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		return job.waitForCompletion(true) ? 0:1;
	}
	public static void main(String[] args) throws Exception{
		int exitcode = ToolRunner.run(new nlinetool(), args);
		System.exit(exitcode);
	}
}
