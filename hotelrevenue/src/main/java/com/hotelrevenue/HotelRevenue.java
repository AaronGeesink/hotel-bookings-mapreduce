package com.hotelrevenue;
import java.io.IOException;
import java.io.StringReader;
import java.text.DateFormatSymbols;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;
import com.opencsv.CSVReader;

public class HotelRevenue {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, FloatWritable>{

    private Text yearMonthPair = new Text();
    private final static FloatWritable revenue = new FloatWritable();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      try {
        CSVReader R = new CSVReader(new StringReader(value.toString()));
        String[] ParsedLine = R.readNext();

        //customer-reservations.csv
        //System.out.print(ParsedLine[9].toString());
        if(ParsedLine[9].equals("Canceled") || ParsedLine[9].equals("Not_Canceled")) {
          //System.out.print("customer-reservations.csv");
          int numDays = Integer.parseInt(ParsedLine[1]) + Integer.parseInt(ParsedLine[2]);
          float revenuePerDay = Float.parseFloat(ParsedLine[8]);
          float totalRevenue = numDays * revenuePerDay;

          String month = new DateFormatSymbols().getMonths()[Integer.parseInt(ParsedLine[5])-1];
          String year = ParsedLine[4];

          revenue.set(totalRevenue);
          yearMonthPair.set(year + "-" + month);
          context.write(yearMonthPair, revenue);
        }
        else { // hotel-booking.csv
          //System.out.print("hotel-booking.csv");
          int numDays = Integer.parseInt(ParsedLine[7]) + Integer.parseInt(ParsedLine[8]);
          float revenuePerDay = Float.parseFloat(ParsedLine[11]);
          float totalRevenue = numDays * revenuePerDay;

          String month = ParsedLine[4];
          String year = ParsedLine[3];

          revenue.set(totalRevenue);
          yearMonthPair.set(year + "-" + month);
          context.write(yearMonthPair, revenue);
        }
        
        R.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
    }
  }

  public static class FloatSumReducer
       extends Reducer<Text,FloatWritable,Text,FloatWritable> {
    private FloatWritable result = new FloatWritable();

    public void reduce(Text key, Iterable<FloatWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      float sum = 0;
      for (FloatWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    BasicConfigurator.configure();
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "hotel revenue");
    job.setJarByClass(HotelRevenue.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(FloatSumReducer.class);
    job.setReducerClass(FloatSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}