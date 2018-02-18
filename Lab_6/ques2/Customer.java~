
import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
public class Customer{
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
	public static void main(String[] args) throws Exception
	{
		Configuration conf=new Configuration();
		conf.setInt(NLineInputFormat.LINES_PER_MAP, 3);
		Job job = new Job(conf,"Customer");
		job.setJarByClass(Customer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(Map.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(NLineInputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.waitForCompletion(true);
	}
}

