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

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(Driver.class);
        job.setJobName("Driver");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(PairOfStrings.class);
        job.setMapOutputValueClass(PairOfStrings.class);
        job.setMapperClass(LineItemMapper1.class);
        job.setReducerClass(Reducer1.class);
        job.setPartitionerClass(Partitioner1.class);
        job.setGroupingComparatorClass(GroupingComparator1.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean status = job.waitForCompletion(true);
        THE_LOGGER.info("run(): status=" + status);
        return status ? 0 : 1;
    }
    public static void main(String[] args) throws Exception {
       if (args.length != 2) {
         throw new IllegalArgumentException
                                   ("usage: <input> <output>");
       }
       THE_LOGGER.info("inputDir = " + args[0]);
       THE_LOGGER.info("outputDir = " + args[1]);
       int returnStatus = ToolRunner.
                     run(new Driver(), args);
       THE_LOGGER.info("returnStatus=" + returnStatus);
       System.exit(returnStatus);
    }
}

