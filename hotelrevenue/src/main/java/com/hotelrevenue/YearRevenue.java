package com.hotelrevenue;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class YearRevenue {
    public static class SortMapper extends Mapper<Text, Text, FloatWritable, Text> {
		public void map(Text rev, Text monthYear, Context context) throws IOException, InterruptedException {
			float revenue = Float.parseFloat(rev.toString());
            String[] tokens = monthYear.toString().split("-"); // This is the delimiter between
			context.write(new FloatWritable(revenue), monthYear);
		}
	}

    public static class MapTask extends
   Mapper<LongWritable, Text, IntWritable, IntWritable> {
    public void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split(","); // This is the delimiter between
        int keypart = Integer.parseInt(tokens[0]);
        int valuePart = Integer.parseInt(tokens[1]);
        context.write(new IntWritable(valuePart), new IntWritable(keypart));

  }
 }
}
