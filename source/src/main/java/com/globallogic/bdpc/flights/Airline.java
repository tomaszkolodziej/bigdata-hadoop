package com.globallogic.bdpc.flights;

public class Airline {

    private String code;
    private String name;

    private Airline() {
        // empty
    }

    public Airline(String code, String name) {
        this.code = code;
        this.name = name;
    }

    public static Airline unrecognizedAirline(String code) {
        return new Airline(code, code);
    }

    public String getCode() {
        return code;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return String.format("Airline %s [%s]", name, code);
    }

}
