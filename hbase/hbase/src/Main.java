
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class Main{


    public static void main(String[] args){

        Configuration conf = HBaseConfiguration.create();
        try {
            HBaseAdmin admin = new HBaseAdmin(conf);
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("raw"));
            tableDescriptor.addFamily(new HColumnDescriptor("stock"));
            tableDescriptor.addFamily(new HColumnDescriptor("time"));
            tableDescriptor.addFamily(new HColumnDescriptor("price"));
            if ( admin.isTableAvailable("raw")){
                admin.disableTable("raw");
                admin.deleteTable("raw");
            }
            admin.createTable(tableDescriptor);
            System.out.println("created Table raw");

            Job job = Job.getInstance();
            job.setJarByClass(Main.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(Job1.Map.class);
            TableMapReduceUtil.initTableReducerJob("raw", null, job);
            job.setNumReduceTasks(0);
            
            
            //create a new job for computing with the raw table.
            Job job2 = Job.getInstance();
            job2.setJarByClass(Job2.class);
            
            HTableDescriptor tableDescriptor2 = new HTableDescriptor(TableName.valueOf("xi_table"));
            tableDescriptor2.addFamily(new HColumnDescriptor("name"));
            tableDescriptor2.addFamily(new HColumnDescriptor("price"));
            if (admin.isTableAvailable("xi_table")){
                admin.disableTable("xi_table");
                admin.deleteTable("xi_table");
            }
            admin.createTable(tableDescriptor2);
            System.out.println("created Table xi_table");
            Scan scan1 = new Scan();
            scan1.setCaching(500);
            scan1.setCacheBlocks(false);
            scan1.addFamily(Bytes.toBytes("stock"));
            scan1.addFamily(Bytes.toBytes("time"));
            scan1.addFamily(Bytes.toBytes("price"));
            TableMapReduceUtil.initTableMapperJob(
                    "raw",
                    scan1, 
                    Job2.Mapper.class,
                    Text.class,
                    Text.class, 
                    job2);
            TableMapReduceUtil.initTableReducerJob(
                    "xi_table", 
                    Job2.Reducer.class,
                    job2);
            job2.setNumReduceTasks(1);
            
            //calculating the volatility
            System.out.println("create Table vol_table");
            Job job3 = Job.getInstance();
            job3.setJarByClass(Job3.class);
            
            HTableDescriptor tableDescriptor3 = new HTableDescriptor(TableName.valueOf("vol_table"));
            tableDescriptor3.addFamily(new HColumnDescriptor("name"));
            tableDescriptor3.addFamily(new HColumnDescriptor("price"));
            if (admin.isTableAvailable("vol_table")){
                admin.disableTable("vol_table");
                admin.deleteTable("vol_table");
            }
            admin.createTable(tableDescriptor3);
            Scan scan3 = new Scan();
            scan3.setCaching(500);
            scan3.setCacheBlocks(false);
            scan3.addFamily(Bytes.toBytes("name"));
            scan3.addFamily(Bytes.toBytes("price"));
            TableMapReduceUtil.initTableMapperJob(
                    "xi_table",
                    scan3, 
                    Job3.Mapper.class,
                    Text.class,
                    Text.class, 
                    job3);
            TableMapReduceUtil.initTableReducerJob(
                    "vol_table", 
                    Job3.Reducer.class,
                    job3);
            job3.setNumReduceTasks(1);
            
          //getting the top 10 and bottom 10
            System.out.println("create Table result_table");
            Job job4 = Job.getInstance();
            job4.setJarByClass(Job4.class);
            
            HTableDescriptor tableDescriptor4 = new HTableDescriptor(TableName.valueOf("result_table"));
            tableDescriptor4.addFamily(new HColumnDescriptor("name"));
            tableDescriptor4.addFamily(new HColumnDescriptor("price"));
            if (admin.isTableAvailable("result_table")){
                admin.disableTable("result_table");
                admin.deleteTable("result_table");
            }
            admin.createTable(tableDescriptor4);
            Scan scan4 = new Scan();
            scan4.setCaching(500);
            scan4.setCacheBlocks(false);
            scan4.addFamily(Bytes.toBytes("name"));
            scan4.addFamily(Bytes.toBytes("price"));
            TableMapReduceUtil.initTableMapperJob(
                    "vol_table",
                    scan4, 
                    Job4.Mapper.class,
                    Text.class,
                    Text.class, 
                    job4);
            TableMapReduceUtil.initTableReducerJob(
                    "result_table", 
                    Job4.Reducer.class,
                    job4);
            job4.setNumReduceTasks(1);
            
            boolean status1 = job.waitForCompletion(true);
            boolean status2 = job2.waitForCompletion(true);
            boolean status3 = job3.waitForCompletion(true);
            boolean status4 = job4.waitForCompletion(true);
            if(status1) {
                
            }
            admin.close();
            System.out.println("end of the program");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
