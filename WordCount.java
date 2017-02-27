import java.io.*;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class WordCount {

	public static class InputMapClass extends Mapper<LongWritable,Text,Text,LongWritable>
	{
		public static final LongWritable a = new LongWritable(1);
		public void map(LongWritable key, Text value, Context context)
		{
			try
			{
				StringTokenizer st = new StringTokenizer(value.toString());
				while(st.hasMoreTokens())
					context.write(new Text(st.nextToken()),a);
			}
			catch(Exception e)
			{
				System.out.println(e.getMessage());
			}
		}
	}

	public static class InputReduceClass extends Reducer<Text,LongWritable,Text,LongWritable>
	{
		private LongWritable result = new LongWritable();

		public void reduce(Text key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {
			long sum = 0;

			for (LongWritable val : values)
			{       	
				sum += val.get();      
			}		 
			result.set(sum);
			context.write(key, result);
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//conf.set("name", "value")
		Job job = Job.getInstance(conf, "Phone Calls");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(InputMapClass.class);
		//job.setCombinerClass(ReduceClass.class);
		job.setReducerClass(InputReduceClass.class);
		//job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}