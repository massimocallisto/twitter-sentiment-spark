package it.unicam.da.streaming.sentiment;

import org.apache.spark.api.java.function.Function;

import java.io.Serializable;

public class MapClass implements Function<String, SentimentModel>, Serializable {
    @Override
    public SentimentModel call(String s) throws Exception {
        SentimentModel model = new SentimentModel();
        model.setSentiment(s);
        return model;
    }
}
