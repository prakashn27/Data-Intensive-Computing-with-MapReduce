

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
//import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/*
 * Sort the results with respect to volatility
 * 
 */
public class Volatile_phase3 {
	
	public static class Map3 extends Mapper<LongWritable, Text, DoubleWritable, Text> {
		private Text value1 = new Text(); //set as the rest element of each line.
		private DoubleWritable uniqueKey = new DoubleWritable(1);
		static enum company { COUNT };
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString(); //receive one line
			String[] element = null;
			element = line.split("\\t");    // since the input is comma seperated
			//key common
			// value company name + volatility
			value1.set(new Text(element[1] + "," + element[0])); 	//Company Name 
			context.write(uniqueKey,  value1);	
		}
	}
	
	public static class CompanyComparator implements Comparator<String> {
		@Override
		public int compare(String o1, String o2) {
			String[] oneArray = o1.split(",");
			String[] twoArray = o2.split(",");
			Double a = Double.parseDouble(oneArray[0]);
			Double b = Double.parseDouble(twoArray[0]);
			return a.compareTo(b);
		}
		
	}
	
	public static class Reduce3 extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {

		private DoubleWritable key2 = new DoubleWritable();
		private Text value2 = new Text();
		
		LinkedList<String> company =new LinkedList<String>();
		int index = 0;
		int top10 = 0;
		int bottom10 = 0;
		boolean isZero;
		boolean boolPrint = true;
		Integer companyCount = null;
		public void reduce(DoubleWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			
			
//			//using single key
			for(Text value : values) {
				String[] oneArray = value.toString().split(",");
				Double d = Double.parseDouble(oneArray[0]);
				if(Double.compare(d, new Double(0)) != 0 && !d.isNaN()) {
					company.add(value.toString());	//add only if not zero
				}
				
			}
			Collections.sort(company, new CompanyComparator());;
			context.write(null,	 new Text("  Top10 with LOW Volatility  "));
			while(top10++ < 10 && !company.isEmpty()) {
				String first = company.removeFirst();
				context.write(null,	 new Text(first));
				
			}
			context.write(null,	 new Text("  "));
			context.write(null,	 new Text(" Top10 with HIGH Volatility "));
			
			while(bottom10++ < 10 && !company.isEmpty()) {
				String last = company.removeLast();
				context.write(null,	 new Text(last));
			} 
							
		}
	}
}
	



