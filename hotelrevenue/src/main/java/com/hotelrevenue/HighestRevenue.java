package com.hotelrevenue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HighestRevenue {
	public static class HighestMapper extends Mapper<Text, Text, Text, Text> {
		public void map(Text monthYear, Text rev, Context context) throws IOException, InterruptedException {
            String[] tokens = monthYear.toString().split("-"); // This is the delimiter between
			String year = tokens[0];
			String yearFragment = year.substring(0, 2);
			String revenueMonthYear = rev.toString() + "-" + tokens[1] + "-" + year;
			context.write(new Text(yearFragment), new Text(revenueMonthYear));
		}
	}

	public static class HighestReducer extends Reducer<Text, Text, Text, FloatWritable> {
		private FloatWritable result = new FloatWritable();

		public void reduce(Text key, Iterable<Text> values, Context context
							) throws IOException, InterruptedException {
			float max = 0;
			String maxMonth = "";
			String maxYear = "";
			for (Text value : values) {
				String[] revenueMonthYear = value.toString().split("-");
				float currentRevenue = Float.parseFloat(revenueMonthYear[0]);
				String currentMonth = revenueMonthYear[1];
				String currentYear = revenueMonthYear[2];
				if (currentRevenue > max) {
					max = currentRevenue;
					maxMonth = currentMonth;
					maxYear = currentYear;
				}
			}
			result.set(max);

			context.write(new Text(maxYear + "-" + maxMonth), result);
		}
	}

	public static boolean calcHighestRevenue(String input, String output) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Highest revenue");
        job.setJarByClass(HotelRevenue.class);
        job.setMapperClass(HighestMapper.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setReducerClass(HighestReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
		
        return(job.waitForCompletion(true));
    }
}
