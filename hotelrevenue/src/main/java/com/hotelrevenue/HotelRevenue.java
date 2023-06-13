package com.hotelrevenue;

public class HotelRevenue {

  public static void main(String[] args) throws Exception {
    boolean success = CalcRevenue.calcRevenue(args);
    if(success) {
      System.out.println("Job 1 Success");
      success = SortRevenue.sortRevenue("output", "output2");
    }
    if (success) {
      System.out.println("Job 2 Success");
      success = YearRevenue.calcYearRevenue("output", "output3");
    }
      System.out.println(success);
  }
}