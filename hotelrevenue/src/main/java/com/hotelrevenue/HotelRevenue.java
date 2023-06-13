package com.hotelrevenue;

public class HotelRevenue {

  public static void main(String[] args) throws Exception {
    boolean success = CalcRevenue.calcRevenue(args);
    if(success) {
      System.out.println("Job 1 Success");
      SortRevenue.sortRevenue("output", "output2");
    }
  }
}