package it.unicam.da.streaming.sentiment;

public class TwitterMapClass extends MapClass{
    //@Override
    public SentimentModel call(SentimentModel s) throws Exception {
        SentimentModel model = new SentimentModel();
        model.setSentiment(s);
        return model;
    }
}
