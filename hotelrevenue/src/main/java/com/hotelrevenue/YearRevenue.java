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

public class YearRevenue {
    public static class YearMapper extends Mapper<Text, Text, Text, Text> {
		public void map(Text monthYear, Text rev, Context context) throws IOException, InterruptedException {
            String[] tokens = monthYear.toString().split("-"); // This is the delimiter between
			String year = tokens[0];
			String revenueMonth = rev.toString() + "-" + tokens[1];
			context.write(new Text(year), new Text(revenueMonth));
		}
	}

	public static class YearReducer extends Reducer<Text, Text, Text, FloatWritable> {
		private FloatWritable result = new FloatWritable();

		public void reduce(Text key, Iterable<Text> values, Context context
							) throws IOException, InterruptedException {
			float max = 0;
			String maxMonth = "";
			for (Text value : values) {
				String[] revenueMonth = value.toString().split("-");
				float currentRevenue = Float.parseFloat(revenueMonth[0]);
				if (currentRevenue > max) {
					max = currentRevenue;
					maxMonth = revenueMonth[1];
				}
			}
			result.set(max);

			context.write(new Text(key.toString() + "-" + maxMonth), result);
		}
	}

	public static boolean calcYearRevenue(String input, String output) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Year revenue");
        job.setJarByClass(HotelRevenue.class);
        job.setMapperClass(YearMapper.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setReducerClass(YearReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
		
        return(job.waitForCompletion(true));
    }
}
