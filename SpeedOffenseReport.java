import java.io.*;

//import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class SpeedOffenseReport {
	
	public static class MapClassSpeed extends Mapper<LongWritable,Text,Text,IntWritable>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try{
	            String[] str = value.toString().split(",");	 
	            int vol = Integer.parseInt(str[1]);
	            //double volume = (vol/1000000);
	            context.write(new Text(str[0]),new IntWritable(vol));
	            
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	
	  public static class ReduceClassSpeed extends Reducer<Text,IntWritable,Text,Text>
	   {
		    //private FloatWritable result = new FloatWritable();
		    
		    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		      //long sum = 0;
		      int countoffense=0,counttotal=0; 
				
		         for (IntWritable val : values)
		         {       
		        	if(val.get()>65)
		        	{
		        		countoffense++;
		        	}
		        	//sum += val.get();
		        	counttotal++;
		         }
		      int results = ((countoffense*100)/counttotal);
		      String percentage = (results+"%");
		      //.set(results);		      
		      context.write(key, new Text(percentage));
		      
		    }
	   }
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    conf.set("mapreduce.output.textoutputformat.separator", ",");
		    Job job = Job.getInstance(conf, "Volume Count in Millions");
		    job.setJarByClass(SpeedOffenseReport.class);
		    job.setMapperClass(MapClassSpeed.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceClassSpeed.class);
		    //job.setNumReduceTasks(2);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(IntWritable.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}
