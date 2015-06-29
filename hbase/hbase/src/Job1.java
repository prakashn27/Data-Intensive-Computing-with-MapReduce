import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
public class Job1 {
	/**
	 * @author ruhansa
	 * read files line by line, put the data into hbase style table
	 * input: <key, value>, key: line number, value: line
	 * output: <key, value>, key: rowid, value: hbase row content
	 */
	public static class Map extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>{
		
		public void map(LongWritable key, Text value, Context context){
			String line = value.toString(); //receive one line
			String element[] = null;
			element = line.split(",");
			if (element[0].trim().compareTo("Date") != 0){
				String dates[]= element[0].split("-");
				byte[] yr = Bytes.toBytes(dates[0]);
				byte[] mm = Bytes.toBytes(dates[1]);
				byte[] dd = Bytes.toBytes(dates[2]);
				byte[] price = Bytes.toBytes(element[6]);
				String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
				String stockName = fileName.substring(0, fileName.length()-4);
				byte[] file = Bytes.toBytes(stockName);
				byte[] rowid = Bytes.toBytes(stockName + key.toString());
//				byte[] rowid = Bytes.toBytes(stockName );
				// set row id as stockname + line number
				Put p = new Put(rowid);
				p.add(Bytes.toBytes("stock"), Bytes.toBytes("name"), file);
				p.add(Bytes.toBytes("time"), Bytes.toBytes("yr"), yr);
				p.add(Bytes.toBytes("time"), Bytes.toBytes("mm"), mm);
				p.add(Bytes.toBytes("time"), Bytes.toBytes("dd"), dd);
				p.add(Bytes.toBytes("price"), Bytes.toBytes("price"), price);
				
				try {
					context.write(new ImmutableBytesWritable(rowid), p);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
}
