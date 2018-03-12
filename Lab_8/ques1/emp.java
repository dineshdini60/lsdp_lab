import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.filecache.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class emp{
	public static class Map extends Mapper<LongWritable,Text,Text,Text> {
		Path[] cfile=new Path[0];
		ArrayList<Text> empl=new ArrayList<Text>();
		public void setup(Context context)
		{
			Configuration conf=context.getConfiguration();
			try
			{
				cfile = DistributedCache.getLocalCacheFiles(conf);
				BufferedReader reader=new BufferedReader(new FileReader(cfile[0].toString()));
				String line;
				while ((line=reader.readLine())!=null)
				{
					Text tt=new Text(line);
					empl.add(tt);	
				}
			}
			catch(IOException e)
			{
				e.printStackTrace();
			}
		}
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] elements = value.toString().split(",");
			for(Text e:empl)
			{
				String[] line1 = e.toString().split(",");
				if(elements[0].equals(line1[0]))
				{
					context.write(new Text(elements[0]),new Text(elements[1]+","+elements[2]+","+line1[1]));
				}
			}  
		}
	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = new Job(conf,"emp");
		job.setJarByClass(emp.class);
		DistributedCache.addCacheFile(new Path(args[0]).toUri(),job.getConfiguration());
		job.setMapperClass(Map.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//job.setInputFormatClass(TextInputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.waitForCompletion(true);
	}
}