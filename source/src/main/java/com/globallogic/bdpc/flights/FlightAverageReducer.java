package com.globallogic.bdpc.flights;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigDecimal;

public class FlightAverageReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    private DoubleWritable averageWritable = new DoubleWritable();

    public void reduce(Text flight, Iterable<IntWritable> delays, Context context) throws IOException, InterruptedException {
        int totalFlights = 0;
        int totalDelays = 0;
        for (IntWritable delay : delays) {
            totalFlights++;
            totalDelays = totalDelays + delay.get();
        }
        BigDecimal averageDelay = BigDecimal.valueOf(totalDelays / totalFlights);
        averageWritable.set(averageDelay.doubleValue());
        context.write(flight, averageWritable);
    }

}
