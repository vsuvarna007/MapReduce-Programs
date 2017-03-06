import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class SeparateString {

	public static class InputMapClass extends Mapper<LongWritable,Text,Text,NullWritable>
	{
		public void map(LongWritable key, Text value, Context context)
		{	    	  
			try{
				String[] round11 = value.toString().split("id");
				String[] round21 = round11[1].split("name");
				String[] round31 = round21[1].split("age");
				String[] round41 = round31[1].split("city");
				String[] round51 = round41[1].split("dept");
				String op = round21[0]+","+round31[0]+","+round41[0]+","+round51[0]+","+round51[1];
				String[] round1 = op.toString().split("\":\"");
				String[] round2_1 = round1[1].split("\",\"");
				String[] round2_2 = round1[2].split("\",\"");
				String[] round3 = round2_2[1].split(",\":");
				String[] round3_1 = round3[1].split(",\",");
				String[] round4 = round1[3].split(" ");
				String[] round4_1 = round4[1].split("\", \",");
				String[] round4_2 = round4_1[0].split("\",");
				String[] round5 = round1[4].split("\"};");
				String op1 = round2_1[0]+","+round2_2[0]+","+round3_1[0]+","+round4_2[0]+","+round5[0];
				context.write(new Text(op1),null);
				
			}
			catch(Exception e)
			{
				System.out.println(e);
			}
		}

		/*public static class InputReduceClass extends Reducer<Text,LongWritable,Text,LongWritable>
		{
			private LongWritable result = new LongWritable();

			public void reduce(Text key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {
				long sum = 0;

				for (LongWritable val : values)
				{       	
					sum += val.get();      
				}		 
				if(sum>60)
				{
					result.set(sum);
					context.write(key, result);
				}
			}
		}*/
	}
		public static void main(String[] args) throws Exception {
			Configuration conf = new Configuration();
			//conf.set("name", "value")
			Job job = Job.getInstance(conf, "Phone SeparateString");
			job.setJarByClass(SeparateString.class);
			job.setMapperClass(InputMapClass.class);
			//job.setCombinerClass(ReduceClass.class);
			//job.setReducerClass(InputReduceClass.class);
			job.setNumReduceTasks(0);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
}