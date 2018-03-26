import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.util.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.input.*;
public class country {
    public static class CompositeKeyWritable implements Writable, WritableComparable<CompositeKeyWritable> {
        private String deptNo;
        private String emp;
        public CompositeKeyWritable() {
        }
        public CompositeKeyWritable(String deptNo, String emp) {
            this.deptNo = deptNo;
            this.emp = emp;
        }
        public String toString() {
            return (new StringBuilder().append(deptNo).append("\t").append(emp)).toString();
        }
        public void readFields(DataInput dataInput) throws IOException {
            deptNo = WritableUtils.readString(dataInput);
            emp = WritableUtils.readString(dataInput);

        }
        public void write(DataOutput dataOutput) throws IOException {
            WritableUtils.writeString(dataOutput,deptNo);
            WritableUtils.writeString(dataOutput,emp);

        }
        public int compareTo(CompositeKeyWritable objKeyPair) {
            int result = deptNo.compareTo(objKeyPair.deptNo);
            if (0 == result) {
                result = emp.compareTo(objKeyPair.emp);
            }
            return result;
        }
    }
    public static class mapper1 extends Mapper<LongWritable, Text,
    CompositeKeyWritable,NullWritable> {
        public void map(LongWritable key, Text value, Context context) throws
        IOException, InterruptedException {
            if (value.toString().length() > 0) {
                String arrEmpAttributes[] = value.toString().split(",");
                context.write(new
                    CompositeKeyWritable(arrEmpAttributes[0].toString(),(arrEmpAttributes[1].toString())),NullWritable.get());
            }
        }
    }
    public static class SecondarySortBasicPartitioner extends Partitioner<CompositeKeyWritable, NullWritable> {
        public int getPartition(CompositeKeyWritable key,NullWritable
            value,int numReduceTasks) {
            return (key.deptNo.hashCode() % numReduceTasks);
        }
    }
    public static class SecondarySortBasicCompKeySortComparator extends WritableComparator {
        protected SecondarySortBasicCompKeySortComparator()
        {
            super(CompositeKeyWritable.class, true);
        }
        public int compare(WritableComparable w1,WritableComparable w2) {
            CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
            CompositeKeyWritable key2 = (CompositeKeyWritable) w2;
            int cmpResult = key1.deptNo.compareTo(key2.deptNo);
            if (cmpResult == 0)
            {
                return -key1.emp.compareTo(key2.emp);
            }
            return cmpResult;
        }
    }
    public static class SecondarySortBasicGroupingComparator extends WritableComparator {
        protected SecondarySortBasicGroupingComparator() {
            super(CompositeKeyWritable.class, true);
        }
        public int compare(WritableComparable w1,WritableComparable w2) {
            CompositeKeyWritable key1 =  (CompositeKeyWritable) w1;
            CompositeKeyWritable key2 = (CompositeKeyWritable) w2;
            return key1.deptNo.compareTo(key2.deptNo);
        }
    }
    public static class SecondarySortBasicReducer extends Reducer<CompositeKeyWritable, NullWritable, CompositeKeyWritable,
    NullWritable> {
        public void reduce(CompositeKeyWritable key, Iterable<NullWritable>
            values, Context context) throws IOException, InterruptedException
        {
            for(NullWritable val:values)
            {
                context.write(key,NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception{

        Configuration conf=new Configuration();
        Job job=new Job(conf,"country");
        job.setJarByClass(country.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(mapper1.class);
        job.setMapOutputKeyClass(CompositeKeyWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setPartitionerClass(SecondarySortBasicPartitioner.class);
        job.setSortComparatorClass(SecondarySortBasicCompKeySortComparator.class);
        job.setGroupingComparatorClass(SecondarySortBasicGroupingComparator.class);
        job.setReducerClass(SecondarySortBasicReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}