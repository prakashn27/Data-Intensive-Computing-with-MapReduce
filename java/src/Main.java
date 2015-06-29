
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class Main {

	public static void main(String[] args) throws Exception {		
		long start = new Date().getTime();		
		Configuration conf = new Configuration();
		
	     Job job = Job.getInstance();
	     job.setJarByClass(Volatile_phase1.class);
	     Job job2 = Job.getInstance();
	     job2.setJarByClass(Volatile_phase2.class);
	     Job job3 = Job.getInstance();
	     job3.setJarByClass(Volatile_phase3.class);
		 

		System.out.println("\n**********Volatility_Hadoop-> Start**********\n");

		job.setJarByClass(Volatile_phase1.class);
		job.setMapperClass(Volatile_phase1.Map1.class);
		job.setReducerClass(Volatile_phase1.Reduce1.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(10);

		job2.setJarByClass(Volatile_phase2.class);
		job2.setMapperClass(Volatile_phase2.Map2.class);
		job2.setReducerClass(Volatile_phase2.Reduce2.class);

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(DoubleWritable.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);
		job2.setNumReduceTasks(10);
		
		
		job3.setJarByClass(Volatile_phase3.class);
		job3.setMapperClass(Volatile_phase3.Map3.class);
//		job3.setCombinerClass(Volatile_phase3.Combine3.class);
		job3.setReducerClass(Volatile_phase3.Reduce3.class);

		job3.setMapOutputKeyClass(DoubleWritable.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setNumReduceTasks(1);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		FileOutputFormat.setOutputPath(job, new Path("Inter_1"));
		FileInputFormat.addInputPath(job2, new Path("Inter_1"));
		
		FileOutputFormat.setOutputPath(job2, new Path("Inter_2"));
		FileInputFormat.addInputPath(job3, new Path("Inter_2"));
		
		FileOutputFormat.setOutputPath(job3, new Path("Inter_3"));
		
		//set the output path
		FileOutputFormat.setOutputPath(job3, new Path(args[1]));
		
		job.waitForCompletion(true);
		job2.waitForCompletion(true);
		boolean status = job3.waitForCompletion(true);
		
		if (status == true) {
			long end = new Date().getTime();
			System.out.println("\nJob took " + (end-start)/1000 + "seconds\n");
		}
		System.out.println("\n**********Volatile_check_Hadoop-> End**********\n");		
	}
}

