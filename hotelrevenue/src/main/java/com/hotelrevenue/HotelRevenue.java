package com.hotelrevenue;

public class HotelRevenue {

  public static void main(String[] args) throws Exception {
    boolean success = CalcRevenue.calcRevenue(args);
    if(success) {
      System.out.println("Job 1 Success (Calc Revenue)");
      success = SortRevenue.sortRevenue("output", "output2");
    }
    if (success) {
      System.out.println("Job 2 Success (Sort Revenue)");
      success = YearRevenue.calcYearRevenue("output", "output3");
    }
    if (success) {
      System.out.println("Job 3 Success (Highest Revenue By Year)");
      success = HighestRevenue.calcHighestRevenue("output3", "output4");
    }
    if (success) {
      System.out.println("Job 4 Success (Highest Revenue Month)");
      System.out.println("All Jobs Successful!");
    }
  }
}