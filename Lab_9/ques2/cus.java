import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class cus {
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] line=value.toString().split(",");
			context.write(new Text(line[0]),new Text(line[1] + " " + line[2]));
		}
	}
	public static class Map2 extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] line=value.toString().split(",");
			context.write(new Text(line[0]),new Text(line[1]));
		}
	}
	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException{
			String line = null;
			for(Text val:values)
			{
				line = val.toString();
			}
			context.write(key, new Text(line));
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "cus");
		job.setJarByClass(cus.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(Map1.class);
		job.setMapperClass(Map2.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);	
		MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class, Map1.class);
		MultipleInputs.addInputPath(job,new Path(args[1]), TextInputFormat.class, Map2.class);	
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.waitForCompletion(true);
	}
}