package com.globallogic.bdpc.flights;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightDelayMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text airlineCodeText = new Text();
    private IntWritable departureDelayWritable = new IntWritable();

    @Override
    public void map(LongWritable lineIndex, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");
        String airlineCode = fields[4];
        int departureDelay = Integer.valueOf(fields[11]);

        airlineCodeText.set(airlineCode);
        departureDelayWritable.set(departureDelay);
        context.write(airlineCodeText, departureDelayWritable);
    }

}
