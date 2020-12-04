package it.unicam.da.streaming.sentiment;

import com.stanford_nlp.model.SentimentResult;
import junit.framework.TestCase;
import org.junit.Assert;

public class SentimentTest extends TestCase {

    public void testAnalyze() {
        String text = "I'm vary tired";
        new Sentiment().analyze(text);
        Assert.assertTrue(true);

    }

    public void testGetSentiment() {
        String text = "I'm vary tired";
        SentimentResult result = new Sentiment().analyze(text);
        System.out.println(Sentiment.getSentiment(result));
        Assert.assertTrue(true);
    }
}