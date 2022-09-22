package com.globallogic.bdpc.flights;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Flights {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: flights <in> <out>");
            System.exit(2);
        }

        Configuration configuration = new Configuration();

        Job topDelaysJob = Job.getInstance(configuration, "top-delays-job");
        topDelaysJob.setJarByClass(Flights.class);
        topDelaysJob.setMapperClass(FlightDelayMapper.class);
        topDelaysJob.setReducerClass(FlightAverageReducer.class);
        topDelaysJob.setOutputKeyClass(Text.class);
        topDelaysJob.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(topDelaysJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(topDelaysJob, new Path(args[1]));

        System.exit(topDelaysJob.waitForCompletion(true) ? 0 : 1);
    }

}
