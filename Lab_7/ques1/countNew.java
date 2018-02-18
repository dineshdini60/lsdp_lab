import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class countNew{
	public static class Map extends Mapper<LongWritable, Text, NullWritable, Text>{
		private TreeMap<Integer, Text> salary = new TreeMap<Integer, Text>();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] elements=line.split(",");
			int i= Integer.parseInt(elements[1]);
			salary.put(i,new Text(value));
			if (salary.size() > 10) {
				//keeping the size as 10 and always removing the smallest element
				salary.remove(salary.firstKey());
				}
			}

			protected void cleanup(Context context) throws IOException, InterruptedException{
				for ( Text name : salary.values() ) {
					context.write(NullWritable.get(), name);
				}
			}

		}

	public static class Reduce extends Reducer<NullWritable, Text, NullWritable, Text>{
		public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException,InterruptedException {
			TreeMap<Integer, Text> salary = new TreeMap< Integer,Text>();
			for (Text value : values) {
				String line = value.toString();
				String[] elements=line.split(",");
				int i= Integer.parseInt(elements[1]);
				salary.put(i, new Text(value));
				if (salary.size() > 10) {
					salary.remove(salary.firstKey());
				}
			}
			for (Text t : salary.values()) {
				context.write(NullWritable.get(), t);
			}
		}
	}


	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = new Job(conf,"countNew");
		job.setJarByClass(countNew.class);
		job.setMapperClass(Map.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(Reduce.class);
		//job.setInputFormatClass(TextInputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setNumReduceTasks(1);
		job.waitForCompletion(true);
	}
}
