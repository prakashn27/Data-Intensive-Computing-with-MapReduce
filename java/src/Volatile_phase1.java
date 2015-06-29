
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
//import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/*
 * two approaches:
 * 1) traverse through all the files in the input path
 * 2) Company list is given choose company from the list and proceed further.
 * 
 * Choosing approach 1
 * 
 * Steps:
 * 1) Compute the Xi for each month of the given file.
 * x1 = (Month end adj close price - month begin adj close price) / month beginning adj close price
 * 
 */
public class Volatile_phase1 {
	
	
	public static class Map1 extends Mapper<Object, Text, Text, Text> {
		private Text key1 = new Text();//set as the column of A or row of B.
		private Text value1 = new Text(); //set as the rest element of each line.
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String line = value.toString(); //receive one line
			String element[] = null;
			element = line.split(",");    // since the input is comma seperated
			if(element.length == 7) { //else the input is not in correct format and not the first line
				//get the input file name for appending to the key value
				String fileName = ((FileSplit) context.getInputSplit()).getPath().getName(); //got as AAA.csv
				String[] date = element[0].split("-"); //spiliting the date format
				
				if(date.length >= 3) {
					String companyName = fileName.split("\\.")[0];   //eliminate the .csv
					key1.set(companyName + date[1] + date[0]); 		//companyName + month + year
					value1.set(date[2] + "," + element[6] + "," + companyName); 	//date , month adj close price , companyName
					context.write(key1,  value1);
				}
			}
		}
	}

	public static class Reduce1 extends Reducer<Text, Text, Text, DoubleWritable> {
		private Text key2 = new Text();
		private DoubleWritable value2 = new DoubleWritable();

		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			int beginDate = 32, lastDate = 0;	//setting as maximim
			float beginPrice = 0, lastPrice = 0;
			String companyName = null;
			//get the beginning and end date for the particular month
			for(Text value: values) {
				String[] threeValue;
				threeValue = value.toString().split(","); //date , adj close value , Company Name
				int curDate = Integer.parseInt(threeValue[0]);
				float curPrice = Float.parseFloat(threeValue[1]);
				if(beginDate > curDate) {
					beginDate = curDate;
					beginPrice = curPrice;
				}
				if(lastDate < curDate) {
					lastDate = curDate;
					lastPrice = curPrice;
				}
				companyName = threeValue[2];
			}
			// key name
			//value value , company name
			if(companyName != null) {
				//xi for the respective month and year
				double xi = (lastPrice - beginPrice) / (beginPrice);
				key2.set(new Text(companyName));
				value2.set(xi);
				context.write(key2, value2);
			}
		}	
	}
}
