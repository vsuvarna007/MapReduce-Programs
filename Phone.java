import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Phone {
	
	public static class InputMapClass extends Mapper<LongWritable,Text,Text,LongWritable>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try{
	        	 String[] str = value.toString().split(",");	 
		            String callerid = str[0];
		            String startDate = str[2];
		            String endDate = str[3];
		            long milli = toMilli(endDate) - toMilli(startDate);
		            long minutes = milli/(1000*60);
		            int std = Integer.parseInt(str[4]);
		            if(std==1)
		            context.write(new Text(callerid),new LongWritable(minutes));
	            
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }

		private long toMilli(String startDate) 
		{
			long milli=0;
			try 
			{
				SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				Date d1 = date.parse(startDate);
				milli = (long)d1.getTime();
			} 
			catch (ParseException e) 
			{
				e.printStackTrace();
			}
			return milli;
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
		      if(sum>60)
		      {
		    	result.set(sum);
		      	context.write(key, result);
		      }
		    }
	   }
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    Job job = Job.getInstance(conf, "Phone Calls");
		    job.setJarByClass(Phone.class);
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