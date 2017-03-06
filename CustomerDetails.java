import java.io.*;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class CustomerDetails {


	public static class InputMapClass extends Mapper<LongWritable,Text,Text,DoubleWritable>
	{
		public void map(LongWritable key, Text value, Context context)
		{
			try
			{
				String data[] = value.toString().split("\\$");
				String name = data[2];
				String age = data[3];
				double amt = Double.parseDouble(data[4]);
				String value1 = name+","+age;
				context.write(new Text(value1),new DoubleWritable(amt));
			}
			catch(Exception e)
			{
				System.out.println(e);
			}
		}
	}
	
	public static class CaderPartitioner extends
	   Partitioner < Text, DoubleWritable >
	   {
	      @Override
	      public int getPartition(Text key, DoubleWritable value, int numReduceTasks)
	      {
	         String[] data = key.toString().split(",");
	         int age = Integer.parseInt(data[1]);
	         if(age>25)
	         {
	            return 0;
	         }
	         else
	         {
	            return 1 ;
	         }
	      }
	   }

	public static class InputReduceClass extends Reducer<Text,DoubleWritable,Text,DoubleWritable>
	{
		
		public void reduce(Text key, Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException 
		{
			double total_amt=0.0;
			DoubleWritable result = new DoubleWritable();
			for (DoubleWritable val : values)
			{       	
				double amt = val.get();
				total_amt+=amt;
			}

			result.set(total_amt);
			context.write(key, result);
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapreduce.output.textoutputformat.separator", ",");
		Job job = Job.getInstance(conf, "Customer Details");
		
		job.setJarByClass(CustomerDetails.class);
		job.setMapperClass(InputMapClass.class);
		
		//job.setCombinerClass(InputReduceClass.class);
		job.setReducerClass(InputReduceClass.class);
		job.setPartitionerClass(CaderPartitioner.class);
		job.setNumReduceTasks(2);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}