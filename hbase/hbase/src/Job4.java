import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class Job4 {
	
	static class Mapper extends
	TableMapper<Text, Text> {
		private Text key1 = new Text("UNIQUE"); //set as the rest element of each line.
		public void map(ImmutableBytesWritable rowId, Result columns,
		Context context) throws IOException {
			try {
				byte[] bName = columns.getValue(Bytes.toBytes("name"), Bytes.toBytes("name"));
				byte[] bPrice = columns.getValue(Bytes.toBytes("price"), Bytes.toBytes("price"));
				
				String name = new String(bName);
				String price = new String(bPrice);
				
				String value = name + "#" + price;	
				System.out.println("mapper4:" + key1.toString() + "::" + value);
				
				context.write(key1, new Text(value));
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	 public static class CompanyComparator implements Comparator<String> {
	        @Override
	        public int compare(String o1, String o2) {
	            String[] oneArray = o1.split("#");
	            String[] twoArray = o2.split("#");
	            Double a = Double.parseDouble(oneArray[1]);
	            Double b = Double.parseDouble(twoArray[1]);
	            return a.compareTo(b);
	        }
	        
	    }
		
	
	public static class Reducer extends
		TableReducer<Text, Text, ImmutableBytesWritable> {
        private DoubleWritable key2 = new DoubleWritable();
        private Text value2 = new Text();
        
        LinkedList<String> company =new LinkedList<String>();
        int index = 0;
        int top10 = 0;
        int bottom10 = 0;
        boolean isZero;
        boolean boolPrint = true;
        Integer companyCount = null;
		public void reduce(Text key,
				Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			 for(Text value : values) {
	             String[] oneArray = value.toString().split("#");	//name + "#" + price;
	             Double d = Double.parseDouble(oneArray[1]);
	             if(Double.compare(d, new Double(0)) != 0 && !d.isNaN()) {
	                 company.add(value.toString());  //add only if not zero
	                 System.out.println(value);
	             }
	             
	         }
	         Collections.sort(company, new CompanyComparator());;
	         System.out.println("  Top10 with LOW Volatility  ");
	         int counter = 10;
	         while(top10++ < 10 && !company.isEmpty()) {
	             String first = company.removeFirst();
	             String name = first.split("#")[0];
	             String value = first.split("#")[1];
	             
	             Put put = new Put(Bytes.toBytes(Integer.toString(++counter)));
//	             Put put = new Put(Bytes.toBytes(name));
	             System.out.println("adding to result table" + name + "::" + value);
	             put.add(Bytes.toBytes("name"), Bytes.toBytes("name"), Bytes.toBytes(name));
	             put.add(Bytes.toBytes("price"), Bytes.toBytes("price"), Bytes.toBytes(value));
//	             byte[] rowid = Bytes.toBytes(key.toString());
	             byte[] rowid = Bytes.toBytes(Integer.toString(counter));
	             context.write(new ImmutableBytesWritable(rowid), put);
	         }
	         while(bottom10++ < 10 && !company.isEmpty()) {
	             String last = company.removeLast();
	             String name = last.split("#")[0];
	             String value = last.split("#")[1];
	             Put put = new Put(Bytes.toBytes(Integer.toString(++counter)));
//	             Put put = new Put(Bytes.toBytes(name));
	             System.out.println("adding to result table" + name + "::" + value);
	             put.add(Bytes.toBytes("name"), Bytes.toBytes("name"), Bytes.toBytes(name));
	             put.add(Bytes.toBytes("price"), Bytes.toBytes("price"), Bytes.toBytes(value));
	             byte[] rowid = Bytes.toBytes(Integer.toString(counter));
	             context.write(new ImmutableBytesWritable(rowid), put);
	         } 
//			Put put = new Put(Bytes.toBytes(key.toString())); //name
//			put.add(Bytes.toBytes("price"), Bytes.toBytes("price"), Bytes.toBytes(sValue)); 
//			System.out.println("reducer3: "+ key + "::" + volatility);
//			context.write(null, put);
		}
	}

}
