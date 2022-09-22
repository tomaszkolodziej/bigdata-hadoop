package com.globallogic.bdpc.flights;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;
import java.util.TreeMap;

import static com.globallogic.bdpc.flights.Airline.unrecognizedAirline;

public class FlightAverageReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private final int LIMIT = 5;
    private final AirlinesMap AIRLINES_MAP = new AirlinesMap();

    private TreeMap<Double, String> top = new TreeMap<>();
    private Text airlineText = new Text();
    private DoubleWritable averageWritable = new DoubleWritable();

    public void reduce(Text airlineCode, Iterable<DoubleWritable> delays, Context context) throws IOException, InterruptedException {
        int totalFlights = 0;
        double totalDelays = 0.0d;

        for (DoubleWritable delay : delays) {
            totalFlights++;
            totalDelays = totalDelays + delay.get();
        }

        BigDecimal averageDelay = BigDecimal.valueOf(totalDelays / totalFlights);
        top.put(averageDelay.doubleValue(), airlineCode.toString());
    }

    @Override
    protected void cleanup(Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        while (top.size() > LIMIT) {
            top.remove(top.lastKey());
        }
        for (Map.Entry<Double, String> entry : top.entrySet()) {
            String airlineCode = entry.getValue();
            Airline airline = AIRLINES_MAP.getOrDefault(airlineCode, unrecognizedAirline(airlineCode));

            airlineText.set(airline.toString());
            averageWritable.set(entry.getKey());
            context.write(airlineText, averageWritable);
        }
    }

}
