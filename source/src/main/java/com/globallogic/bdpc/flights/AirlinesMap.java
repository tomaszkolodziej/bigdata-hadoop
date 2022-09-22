package com.globallogic.bdpc.flights;

import java.util.HashMap;

public class AirlinesMap extends HashMap<String, Airline> {

    public AirlinesMap() {
        put(new Airline("UA", "United Air Lines Inc."));
        put(new Airline("AA", "American Airlines Inc."));
        put(new Airline("US", "US Airways Inc."));
        put(new Airline("F9", "Frontier Airlines Inc."));
        put(new Airline("B6", "JetBlue Airways"));
        put(new Airline("00", "Skywest Airlines Inc."));
        put(new Airline("AS", "Alaska Airlines Inc."));
        put(new Airline("NK", "Spirit Air Lines Co."));
        put(new Airline("WN", "Southwest Airlines Co."));
        put(new Airline("DL", "Delta Air Lines Inc."));
        put(new Airline("EV", "Atlantic Southeast Airlines"));
        put(new Airline("HA", "Hawaiian Airlines Inc."));
        put(new Airline("MQ", "American Eagle Airlines Inc."));
        put(new Airline("VX", "Virgin America"));
    }

    private void put(Airline airline) {
        put(airline.getCode(), airline);
    }

}
