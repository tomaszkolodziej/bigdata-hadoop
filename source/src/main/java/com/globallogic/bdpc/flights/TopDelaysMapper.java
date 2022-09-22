package com.globallogic.bdpc.flights;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import static com.globallogic.bdpc.flights.Airline.unrecognizedAirline;

public class TopDelaysMapper extends Mapper<Text, DoubleWritable, Text, DoubleWritable> {

    private final int LIMIT = 5;
    private final AirlinesMap AIRLINES_MAP = new AirlinesMap();

    private TreeMap<Double, Text> top = new TreeMap<>();
    private Text airlineText = new Text();
    private DoubleWritable averageWritable = new DoubleWritable();

    @Override
    public void map(Text airlineCode, DoubleWritable averageDepartureDelay, Context context) throws IOException, InterruptedException {
        top.put(averageDepartureDelay.get(), airlineCode);
        if (top.size() > LIMIT) {
            top.remove(top.lastKey());
        }
    }

    @Override
    protected void cleanup(Mapper<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        for (Map.Entry<Double, Text> entry : top.entrySet()) {
            String airlineCode = entry.getValue().toString();
            Airline airline = AIRLINES_MAP.getOrDefault(airlineCode, unrecognizedAirline(airlineCode));

            airlineText.set(airline.toString());
            averageWritable.set(entry.getKey());
            context.write(airlineText, averageWritable);
        }
    }

}
