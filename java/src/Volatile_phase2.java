import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Volatile_phase2 {
	static enum counter { COUNT };
	static int companyCount = 0;	//count the company list
	public static class Map2 extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		private Text key1 = new Text();// set as the row and column of C
		private DoubleWritable value1 = new DoubleWritable(); // set as the baby value(incomplete
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString(); //receive one line
			String element[] = null;
			element = line.split("\\t"); 
			if(element.length == 2) {
				key1.set(element[0]);
				value1.set(Double.parseDouble(element[1]));
				context.write(key1, value1);	//company name and xi
			}
		}
	}

	public static class Reduce2 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			companyCount++; 	//increment the company count
			
			Double sum = 0.0, N = 0.0;
			LinkedList<Double> tempList = new LinkedList<Double>();
			// copy the value
			for (DoubleWritable value : values) {
				tempList.add(value.get());
			}
			// calculate summation xi
			for (Double x: tempList) {
				sum += x;
				N++;
			}
			Double mean = sum / N;
			Double v = 0.0;
			for (Double value : tempList) {
				v += Math.pow(value - mean, 2);
			}
			Double volatility = Math.sqrt((v) / (N - 1));

			// key company Name
			// value volatility
			context.write(key, new DoubleWritable(volatility));
		}
	}
}
