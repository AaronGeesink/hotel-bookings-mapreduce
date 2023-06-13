package com.hotelrevenue;

public class HotelRevenue {

  public static void main(String[] args) throws Exception {
    long startTime = System.nanoTime();
    boolean success = CalcRevenue.calcRevenue(args);
    long endTime = System.nanoTime();
    long durationInMilliseconds = (endTime - startTime)/1000000;

    if(success) {
      System.out.println("Job 1 Success (Calc Revenue)");
      System.out.println ("Time in milliseconds: " + durationInMilliseconds);

      startTime = System.nanoTime();
      success = SortRevenue.sortRevenue("output", "output2");
      endTime = System.nanoTime();
      durationInMilliseconds = (endTime - startTime)/1000000;
    }
    if (success) {
      System.out.println("Job 2 Success (Sort Revenue)");
      System.out.println ("Time in milliseconds: " + durationInMilliseconds);

      startTime = System.nanoTime();
      success = YearRevenue.calcYearRevenue("output", "output3");
      endTime = System.nanoTime();
      durationInMilliseconds = (endTime - startTime)/1000000;
    }
    if (success) {
      System.out.println("Job 3 Success (Highest Revenue By Year)");
      System.out.println ("Time in milliseconds: " + durationInMilliseconds);

      startTime = System.nanoTime();
      success = HighestRevenue.calcHighestRevenue("output3", "output4");
      endTime = System.nanoTime();
      durationInMilliseconds = (endTime - startTime)/1000000;
    }
    if (success) {
      System.out.println("Job 4 Success (Highest Revenue Month)");
      System.out.println ("Time in milliseconds: " + durationInMilliseconds);
      System.out.println("All Jobs Successful!");
    }
  }
}