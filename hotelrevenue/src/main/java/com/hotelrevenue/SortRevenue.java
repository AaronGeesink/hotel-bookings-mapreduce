package com.hotelrevenue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SortRevenue {
    public static class SortMapper extends Mapper<Text, Text, FloatWritable, Text> {
		@Override
		public void map(Text monthYear, Text rev, Context context) throws IOException, InterruptedException {
			float revenue = Float.parseFloat(rev.toString());
			context.write(new FloatWritable(revenue), monthYear);
		}
	}

  public static class FloatComparator extends WritableComparator {

		public FloatComparator() {
			super(FloatWritable.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			FloatWritable key1 = (FloatWritable) w1;
			FloatWritable key2 = (FloatWritable) w2;          
			return -1 * key1.compareTo(key2);
		}
	}

  public static boolean sortRevenue(String input, String output) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "hotel revenue");
    job.setJarByClass(HotelRevenue.class);
    job.setMapperClass(SortMapper.class);
    job.setInputFormatClass(KeyValueTextInputFormat.class);
    job.setMapOutputKeyClass(FloatWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setSortComparatorClass(FloatComparator.class);
    job.setReducerClass(Reducer.class);
    job.setNumReduceTasks(1);
    FileInputFormat.setInputPaths(job, new Path(input));
    FileOutputFormat.setOutputPath(job, new Path(output));
    return(job.waitForCompletion(true));
  }
}
