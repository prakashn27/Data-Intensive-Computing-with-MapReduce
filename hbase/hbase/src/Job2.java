//import java.io.IOException;
//
//import org.apache.hadoop.io.FloatWritable;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.lib.input.FileSplit;
//import org.apache.hadoop.hbase.KeyValue;
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
//import org.apache.hadoop.hbase.mapreduce.TableMapper;
//import org.apache.hadoop.hbase.mapreduce.TableReducer;
//import org.apache.hadoop.hbase.util.Bytes;
//
//public class Job2 {
//
//    static class Mapper extends
//            TableMapper<Text, Text> {
//
//        public void map(ImmutableBytesWritable row, Result columns,
//                Context context) throws IOException {
//                try {
//                byte[] bName = columns.getValue(Bytes.toBytes("stock"), Bytes.toBytes("name"));
//                byte[] bYear = columns.getValue(Bytes.toBytes("time"), Bytes.toBytes("yr"));
//                byte[] bMonth = columns.getValue(Bytes.toBytes("time"), Bytes.toBytes("mm"));
//                byte[] bDate = columns.getValue(Bytes.toBytes("time"), Bytes.toBytes("dd"));
//                byte[] bPrice = columns.getValue(Bytes.toBytes("price"), Bytes.toBytes("price"));
//                
//                String name = new String(bName);
//                String year = new String(bYear);
//                String month = new String(bMonth);
//                String date = new String(bDate);
//                String price = new String(bPrice);
//                
//                String key = name + "#" + year + "#" + month;
//                String value = date + "#" + price;  
//                System.out.println(key + "::" + value);
//                context.write(new Text(key), new Text(value));
//            } catch (InterruptedException e) {
//                // TODO Auto-generated catch block
//                e.printStackTrace();
//            }
//        }
//    }
//
//    public static class Reducer
//            extends
//            TableReducer<Text, Text, ImmutableBytesWritable> {
//
//        public void reduce(ImmutableBytesWritable key,
//                Iterable<Text> values, Context context)
//                throws IOException, InterruptedException {
//        		System.out.println("MYIR");
//            try {
//                int beginDate = 32, lastDate = 0;   //setting as maximim
//                float beginPrice = 0, lastPrice = 0;
//                String companyName = key.toString().split("#")[0];
//                for(Text value: values) {
//                    String[] threeValue;
//                    threeValue = value.toString().split("#"); //date + adj close value
//                    int curDate = Integer.parseInt(threeValue[0]);
//                    float curPrice = Float.parseFloat(threeValue[1]);
//                    if(beginDate > curDate) {
//                        beginDate = curDate;
//                        beginPrice = curPrice;
//                    }
//                    if(lastDate < curDate) {
//                        lastDate = curDate;
//                        lastPrice = curPrice;
//                    }
//                }
//
//                //xi for the respective month and year
//                float xi = (lastPrice - beginPrice) / (beginPrice); 
//                System.out.println("reducer2: " + key.toString() + "::" +xi);
//                String sValue = String.valueOf(xi);
//                Put put = new Put(Bytes.toBytes(key.toString())); //name + "#" + year + "#" + month;
//                put.add(Bytes.toBytes("name"), Bytes.toBytes("name"), Bytes.toBytes(companyName));
//                put.add(Bytes.toBytes("price"), Bytes.toBytes("price"), Bytes.toBytes(sValue));     //value: xi
//                
//                context.write(new ImmutableBytesWritable(key), put);
//                
//                
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//
//        }
//    }
//}
//

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;


public class Job2{
    
    public static class Mapper extends TableMapper<Text, Text>{
                
    public void map(ImmutableBytesWritable row, Result columns, Context context) throws InterruptedException, IOException {
        
    byte[] bName = columns.getValue(Bytes.toBytes("stock"), Bytes.toBytes("name"));
      byte[] bYear = columns.getValue(Bytes.toBytes("time"), Bytes.toBytes("yr"));
      byte[] bMonth = columns.getValue(Bytes.toBytes("time"), Bytes.toBytes("mm"));
      byte[] bDate = columns.getValue(Bytes.toBytes("time"), Bytes.toBytes("dd"));
      byte[] bPrice = columns.getValue(Bytes.toBytes("price"), Bytes.toBytes("price"));
      
      String name = new String(bName);
      String year = new String(bYear);
      String month = new String(bMonth);
      String date = new String(bDate);
      String price = new String(bPrice);
      
      String key = name + "#" + year + "#" + month;
      String value = date + "#" + price;  
      System.out.println(key + "::" + value);
      context.write(new Text(key), new Text(value));
    
     }
    }
    
    public static class Reducer extends TableReducer<Text, Text, ImmutableBytesWritable>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        	int beginDate = 32, lastDate = 0;   //setting as maximim
          float beginPrice = 0, lastPrice = 0;
          String companyName = key.toString().split("#")[0];
          for(Text value: values) {
              String[] threeValue;
              threeValue = value.toString().split("#"); //date + adj close value
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
          }

          //xi for the respective month and year
          float xi = (lastPrice - beginPrice) / (beginPrice); 
            String closebalance= ""+xi; 
            System.out.println(key);
            System.out.println(closebalance);
             byte[] rowid = Bytes.toBytes(key.toString());
             Put put = new Put(rowid); //name + "#" + year + "#" + month;
             
             put.add(Bytes.toBytes("name"), Bytes.toBytes("name"), Bytes.toBytes(key.toString()));
             put.add(Bytes.toBytes("price"), Bytes.toBytes("price"), Bytes.toBytes(closebalance)); 
             context.write(new ImmutableBytesWritable(rowid), put);
            
        }
    }
}
