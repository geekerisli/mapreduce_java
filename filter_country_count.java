import java.io.IOException;

import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Countrycounter_TASKB {
	
	
	
	public static class MyPage_index{	 
		public static final int USER_ID = 0;
		public static final int USER_NAME = 1;
		public static final int Nationality = 2;
		public static final int CountryCode = 3;
		public static final int Hobby = 4;
		//in next line input the country that need to be find
		public static final String NationalityChoose = "F9FHEX2RZ9V83V3BQ6";
					 
	}
	
	
	
	public static class countrySelectionMapper
		extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String Line = value.toString();
			String colle[] = Line.split(",");
			context.write(new Text(colle[MyPage_index.Nationality]), new IntWritable(1));
			
			
						
		}
	}
	
	public static class countrySelectionReducer
		extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
						
			//strange question *2
			int sum=0;
		      for (IntWritable val : values) {
		        sum += 1;
		      }
		      
		      context.write(key, new IntWritable(sum));
			
		}
	}
	
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    if (args.length != 2) {
				System.err.println("Usage: Facebook Users <input path> <output path>"); 
				System.exit(-1); 
				}
			Job job = new Job();
			job.setJarByClass(Countrycounter_TASKB.class);
			job.setJobName("Find Facebook User");
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.setMapperClass(countrySelectionMapper.class);
			job.setReducerClass(countrySelectionReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
		    job.setNumReduceTasks(1);
		    job.setOutputValueClass(IntWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	}
