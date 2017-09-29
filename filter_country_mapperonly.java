import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Countrycounter_TASKA {
	
	
	
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
			
			if(colle[MyPage_index.Nationality].equals(MyPage_index.NationalityChoose)){
				context.write(new Text(colle[MyPage_index.USER_ID]), new IntWritable(1));
			
			}
						
		}
	}
