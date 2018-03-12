import java.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
public class student{
	public static class Map extends Mapper<Text,Text,Text,Text>
	{
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException
		{
			String name = context.getConfiguration().get("name");
			String[] elements = key.toString().split(",");
			if(name.equalsIgnoreCase(elements[1]))
				context.write(key,value);
		}

	}
	public static void main(String[] args) throws Exception
	{
		Configuration conf=new Configuration();
		conf.set("name",args[2]);
		Job job=new Job(conf,"student");
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setJarByClass(student.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(Map.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.waitForCompletion(true);
	}
}