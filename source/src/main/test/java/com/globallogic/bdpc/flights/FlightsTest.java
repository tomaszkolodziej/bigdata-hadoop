package com.globallogic.bdpc.flights;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

import java.io.IOException;

import static java.util.Arrays.asList;

public class FlightsTest {

    @Test
    public void testFlightDelayMapper() throws IOException {
        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new FlightDelayMapper())
                .withAll(asList(
                        new Pair<>(new LongWritable(1L), new Text("2015,1,1,4,AS,98,N407AS,ANC,SEA,0005,2354,-11,21")),
                        new Pair<>(new LongWritable(2L), new Text("2015,1,1,4,AA,2336,N3KUAA,LAX,PBI,0010,0002,-8,12")),
                        new Pair<>(new LongWritable(3L), new Text("2015,1,1,4,US,840,N171US,SFO,CLT,0020,0018,-2,16"))
                ))
                .withAllOutput(asList(
                        new Pair<>(new Text("AS"), new IntWritable(-11)),
                        new Pair<>(new Text("AA"), new IntWritable(-8)),
                        new Pair<>(new Text("US"), new IntWritable(-2))
                ))
                .runTest();
    }

    @Test
    public void testFlightAverageReducer() throws IOException {
        new ReduceDriver<Text, IntWritable, Text, DoubleWritable>()
                .withReducer(new FlightAverageReducer())
                .withAll(asList(
                        new Pair<>(new Text("AS"), asList(new IntWritable(-11), new IntWritable(-13))),
                        new Pair<>(new Text("AA"), asList(new IntWritable(-8), new IntWritable(-10))),
                        new Pair<>(new Text("US"), asList(new IntWritable(-2), new IntWritable(-4), new IntWritable(-8)))
                ))
                .withAllOutput(asList(
                        new Pair<>(new Text("AS"), new DoubleWritable(-12)),
                        new Pair<>(new Text("AA"), new DoubleWritable(-9)),
                        new Pair<>(new Text("US"), new DoubleWritable(-4))
                ))
                .runTest();
    }

    @Test
    public void testTopDelaysMapper() throws IOException {
        new MapDriver<Text, DoubleWritable, Text, DoubleWritable>()
                .withMapper(new TopDelaysMapper())
                .withAll(asList(
                        new Pair<>(new Text("AA"), new DoubleWritable(-9)),
                        new Pair<>(new Text("US"), new DoubleWritable(-4)),
                        new Pair<>(new Text("AS"), new DoubleWritable(-12)),
                        new Pair<>(new Text("ZZ"), new DoubleWritable(-16)),
                        new Pair<>(new Text("CC"), new DoubleWritable(-10)),
                        new Pair<>(new Text("DD"), new DoubleWritable(-30))
                ))
                .withAllOutput(asList(
                        new Pair<>(new Text("Airline DD [DD]"), new DoubleWritable(-30)),
                        new Pair<>(new Text("Airline ZZ [ZZ]"), new DoubleWritable(-16)),
                        new Pair<>(new Text("Airline Alaska Airlines Inc. [AS]"), new DoubleWritable(-12)),
                        new Pair<>(new Text("Airline CC [CC]"), new DoubleWritable(-10)),
                        new Pair<>(new Text("Airline American Airlines Inc. [AA]"), new DoubleWritable(-9))
                ))
                .runTest();
    }

}
