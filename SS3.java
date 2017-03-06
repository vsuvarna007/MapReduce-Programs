import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class SS3 {

	public static class InputMapClass extends Mapper<LongWritable,Text,Text,Text>
	{
		
		public void map(LongWritable key, Text value, Context context)
		{	    	  
			try{
				int a = 1;
				String[] data = value.toString().split(",");
				String name = data[1];
				String age = data[2];
				String city = data[3];
				String dept = data[4];
				String key1 = city+","+dept+","+age+","+a;
				context.write(new Text(name),new Text(key1));
				
			}
			catch(Exception e)
			{
				System.out.println(e);
			}
		}
	}

		public static class InputReduceClass2 extends Reducer<Text,Text,Text,LongWritable>
		{
			private LongWritable result = new LongWritable();

			public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
				long sum = 0;
				int age =0;
				for (Text val : values)
				{       	
					String[] data = val.toString().split(",");
					int a = Integer.parseInt(data[3]);
					age = Integer.parseInt(data[2]);
					String city = data[0];
					sum+=a;
					if(age>28)
					{
						if(city.equals("Bangalore")||city.equals("Mumbai"))
						{
							result.set(sum);
							context.write(key, result);
						}
					}
					
				}
				
			}
		}
	
		public static void main(String[] args) throws Exception {
			Configuration conf = new Configuration();
			//conf.set("name", "value")
			Job job = Job.getInstance(conf, "Phone SeparateString");
			job.setJarByClass(SS3.class);
			job.setMapperClass(InputMapClass.class);
			//job.setCombinerClass(ReduceClass.class);
			job.setReducerClass(InputReduceClass2.class);
			//job.setNumReduceTasks(3xx);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
			}
}