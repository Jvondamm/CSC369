import org.apache.log4j.Logger;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.input.*;

public class Driver extends Configured
                                         implements Tool {
    private static final Logger THE_LOGGER =
             Logger.getLogger(Driver.class);
    private static Path product;
    private static Path lineItem;
    private static Path output;
           
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
          throw new IllegalArgumentException
                                    ("usage: <input1> <input2> <output>");
        }
         product = new Path(args[0]);
         lineItem = new Path(args[1]);
         output = new Path(args[2]);
 
        THE_LOGGER.info("inputDir1 = " + args[0]);
        THE_LOGGER.info("inputDir2 = " + args[1]);
        THE_LOGGER.info("outputDir = " + args[2]);
        int returnStatus = ToolRunner.
                      run(new Driver(), args);
        THE_LOGGER.info("returnStatus=" + returnStatus);
        System.exit(returnStatus);
     }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(Driver.class);
        job.setJobName("JOB 1");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, lineItem, 
        TextInputFormat.class, LineItemMapper1.class);
        MultipleInputs.addInputPath(job, product,
          TextInputFormat.class, ProductMapper1.class);

        job.setMapOutputKeyClass(PairOfStrings.class);
        job.setMapOutputValueClass(PairOfStrings.class);
        job.setReducerClass(Reducer1.class);
        job.setPartitionerClass(Partitioner1.class);
        job.setGroupingComparatorClass(GroupingComparator1.class);

        FileOutputFormat.setOutputPath(job, output);
        boolean status = job.waitForCompletion(true);
        THE_LOGGER.info("run(): status=" + status);
        return status ? 0 : 1;
    }
}