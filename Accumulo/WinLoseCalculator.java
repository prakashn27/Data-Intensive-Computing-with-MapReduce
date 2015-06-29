import java.io.IOException;

import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Parser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WinLoseCalculator extends Configured implements Tool {
  private static Options opts;
  private static Option passwordOpt;
  private static Option usernameOpt;
  private static String USAGE = "WinLoseCalculator <instance name> <zoo keepers> <input dir> <output table>";
  
  static {
    usernameOpt = new Option("u", "username", true, "username");
    passwordOpt = new Option("p", "password", true, "password");
    
    opts = new Options();
    opts.addOption(usernameOpt);
    opts.addOption(passwordOpt);
  }
  /*
  key   :   tagName_win || tagName_lose 
  value :   1
  */
  public static class MapClass extends Mapper<LongWritable,Text,Text,Text> {
      @Override
      public void map(LongWritable key, Text value, Context output) throws IOException {
        String[] lineSplit = value.toString().split(","); 
        //get the tag name
        String fileName = ((FileSplit) output.getInputSplit()).getPath().getName(); //got as AAA.csv
        String tagName = fileName.split("\\.")[0];
        if(lineSplit.length == 2) {
          String[] words = lineSplit[1].split(" ");
          for (String word : words) {
            if(word.trim().toLowerCase().compareTo("win") == 0) { 
              try {
                  output.write(new Text("tagName" + "_win"), new Text("1"));
              } catch (InterruptedException e) {
                  e.printStackTrace();
              }
              // Mutation mutation = new Mutation(new Text(tagName + "_win"));
              // mutation.put(new Text("count"), new Text("20080906"), new Value("1".getBytes()));  
            }
            if(word.trim().toLowerCase().compareTo("lose") == 0) {  
              try {
                  output.write(new Text("tagName" + "_lose"), new Text("1"));
              } catch (InterruptedException e) {
                  e.printStackTrace();
              }
              // Mutation mutation = new Mutation(new Text(tagName + "_lose"));
              // mutation.put(new Text("count"), new Text("20080906"), new Value("1".getBytes()));
            }
          }
        }
        // try {
        //     output.write(null, mutation);
        // } catch (InterruptedException e) {
        //     e.printStackTrace();
        // }
      }
    }
  /*
  key:  
  */
  public static class ReduceClass extends Reducer<Text,Text,Text, Mutation> {
    //TODO: hardcode to seperate into eastern and western conference

    public void reduce(Text key, Iterable<Text> values, Context output) throws IOException, InterruptedException {
      long timestamp = System.currentTimeMillis();
      /*
      Text rowID = new Text("row1");
      Text colFam = new Text("myColFam");
      Text colQual = new Text("myColQual");
      ColumnVisibility colVis = new ColumnVisibility("public");
      long timestamp = System.currentTimeMillis();

      Value value = new Value("myValue".getBytes());

      Mutation mutation = new Mutation(rowID);
      mutation.put(colFam, colQual, colVis, timestamp, value);
      */  
      
      int index = 0;
      String sKey = key.toString();
      String[] keySplit = sKey.split("_");
      if(keySplit.length == 2)  {
        String tag = keySplit[0];
        String status = keySplit[1];  //indicates win or loose
        for (Text value : values) {
            index++;
        }
        Text rowID = key; //new Text(key);
        Text colFam = new Text(tag);  //team name
        Text colQual = new Text(status);  //win or lose status
        ColumnVisibility colVis = new ColumnVisibility("public");
        long timestamp1 = System.currentTimeMillis();

        Value value = new Value(Integer.toString(index).getBytes());

        Mutation mutation = new Mutation(rowID);
        mutation.put(colFam, colQual, colVis, timestamp1, value);
      }
      // Key outputKey = new Key(key, new Text("foo"), new Text("" + index), timestamp);
      // index++;
      // Value outputValue = new Value(value.getBytes(), 0, value.getLength());

      // output.write(outputKey, outputValue);
      
    }
  }
  public int run(String[] unprocessed_args) throws Exception {
    Parser p = new BasicParser();
    
    CommandLine cl = p.parse(opts, unprocessed_args);
    String[] args = cl.getArgs();
    
    String username = cl.getOptionValue(usernameOpt.getOpt(), "root");
    String password = cl.getOptionValue(passwordOpt.getOpt(), "acc");
    
    if (args.length != 4) {
      System.out.println("ERROR: Wrong number of parameters: " + args.length + " instead of 4.");
      return printUsage();
    }
    
    Job job = new Job(getConf(), WinLoseCalculator.class.getName());
    job.setJarByClass(this.getClass());
    
    job.setInputFormatClass(TextInputFormat.class);
    TextInputFormat.setInputPaths(job, new Path(args[2]));
    
    job.setMapperClass(MapClass.class);    
    job.setNumReduceTasks(0);
    
    job.setOutputFormatClass(AccumuloOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Mutation.class);
    AccumuloOutputFormat.setOutputInfo(job.getConfiguration(), username, password.getBytes(), true, args[3]);
    AccumuloOutputFormat.setZooKeeperInstance(job.getConfiguration(), args[0], args[1]);
    job.waitForCompletion(true);
    return 0;
  }
  
  private int printUsage() {
    HelpFormatter hf = new HelpFormatter();
    hf.printHelp(USAGE, opts);
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(CachedConfiguration.getInstance(), new WinLoseCalculator(), args);
    System.exit(res);
  }
}
// class MyReducer extends Reducer<WritableComparable, Writable, Text, Mutation> {

//     public void reduce(WritableComparable key, Iterator<Text> values, Context c) {

//         Mutation m;

//         // create the mutation based on input key and value

//         c.write(new Text("output-table"), m);
//     }
// }