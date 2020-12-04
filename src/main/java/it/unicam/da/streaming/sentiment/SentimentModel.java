package it.unicam.da.streaming.sentiment;

import java.io.Serializable;

public class SentimentModel implements Serializable {
    public String getSentiment() {
        return sentiment;
    }

    public void setSentiment(String sentiment) {
        this.sentiment = sentiment;
    }

    String sentiment;
}
