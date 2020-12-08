package it.unicam.da.streaming.sentiment;

import com.stanford_nlp.model.SentimentResult;
import junit.framework.TestCase;
import org.junit.Assert;

public class SentimentTest extends TestCase {

    public void testAnalyze() {
        String text = "I'm vary tired";
        new Sentiment(text);
        Assert.assertTrue(true);

    }

    public void testGetSentiment() {
        String text = "I'm vary tired";
        Sentiment sentiment = new Sentiment(text);
        System.out.println(sentiment.getSentiment());
        Assert.assertTrue(true);
    }
}