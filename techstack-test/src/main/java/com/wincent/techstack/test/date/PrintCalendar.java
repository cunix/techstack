package com.wincent.techstack.test.date;

public class PrintCalendar {


	public static boolean LeapYear(int year) { // 判断闰年
		if (year % 400 == 0 || (year % 4 == 0 && year % 100 != 0)) { // 是闰年
			return true;
		}else {
			return false;
		}
	}

	public static final int N = 1900; // 用N表示起始年份

	public static void main(String[] args) {
		printCalendar(2017);
	}

	
	
	public static void printCalendar(int Year){
		System.out.println("星期日     " + "星期一     " + "星期二    " + "星期三     " + "星期四     " + "星期五     " + "星期六   ");
		System.out.println();
		int n = 1;// 1900年1月1日 是星期一
		int month[] = { 0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 }; // 用来存储月份的天数
		int year[] = new int[1001]; // 用来存储每年1月1日是星期几
		year[0] = n;
		for (int i = 1; i < year.length; i++) {
			int days = 365;
			if (LeapYear(i + N - 1)) {// 如果它的前一年是闰年则需要加366
				days = 366;
				year[i] = (year[i - 1] + days) % 7;
			} else {
				year[i] = (year[i - 1] + days) % 7;
			}
		}
		int Month=13;
		
	    for (int j = 1; j < Month; j++) {
	    	if (LeapYear(Year)) { // 如果是闰年,2月改为29号
				month[2] = 29;
			}
			int day = 0; // 用来记录当前月的一号是今年的第几天
			for (int i = 1; i < j; i++) {
				day = day + month[i];
			}

			day = (year[Year - N] + day) % 7;
			for (int i = 0; i < 7; i++) {// 输出控制
				if (day == i) {
					System.out.print("  " + 1 + "   ");
					if (day == 6) {
						System.out.println();
					}
					break;
				} else {
					System.out.print("      ");
				}
			}
			for (int i = 2; i <= month[j]; i++) {
				if (i < 10) {
					System.out.print("  " + i + "   ");
				} else {
					System.out.print(" " + i + "   ");
				}
				if ((day + i - 1) % 7 == 6) {
					System.out.println();
				}
			}
		}
		
		
	}
}
