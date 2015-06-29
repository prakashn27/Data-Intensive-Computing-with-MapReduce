import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class Job3 {
	
	static class Mapper extends
	TableMapper<Text, Text> {

		public void map(ImmutableBytesWritable rowId, Result columns,
		Context context) throws IOException {
		try {
	//		byte[] bName = columns.getValue(Bytes.toBytes("name"), Bytes.toBytes("name"));
	//		String oldKey = new String(rowId.get());
	//		String name = oldKey.split("#")[0];
			byte[] bName = columns.getValue(Bytes.toBytes("name"), Bytes.toBytes("name"));
			byte[] bPrice = columns.getValue(Bytes.toBytes("price"), Bytes.toBytes("price"));
			
			String name = new String(bName);
			String price = new String(bPrice);
			
			String key = name.split("#")[0];
			String value = price;	
			System.out.println("mapper3:" + key + "::" + value);
			context.write(new Text(key), new Text(value));
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
}
}

public static class Reducer
	extends
	TableReducer<Text, Text, ImmutableBytesWritable> {

public void reduce(Text key,
		Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
	Double sum = 0.0, N = 0.0;
	LinkedList<Double> tempList = new LinkedList<Double>();
	try {
		
		
		
		// copy the value
		for (Text value : values) {
		    tempList.add(Double.parseDouble(value.toString()));
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
		String sValue = String.valueOf(volatility);
		Put put = new Put(Bytes.toBytes(key.toString())); //name
		put.add(Bytes.toBytes("name"), Bytes.toBytes("name"), Bytes.toBytes(key.toString()));
		put.add(Bytes.toBytes("price"), Bytes.toBytes("price"), Bytes.toBytes(sValue)); 
		System.out.println("reducer3: "+ key + "::" + volatility);
		byte[] rowid = Bytes.toBytes(key.toString());
		context.write(new ImmutableBytesWritable(rowid), put);
	} catch (Exception e) {
		e.printStackTrace();
	}

}
}

}
