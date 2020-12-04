package it.unicam.da.streaming.sentiment;


import com.stanford_nlp.SentimentAnalyzer.SentimentAnalyzer;
import com.stanford_nlp.model.SentimentClassification;
import com.stanford_nlp.model.SentimentResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

public class Sentiment {


    public SentimentResult analyze(String text) {

        SentimentAnalyzer sentimentAnalyzer = new SentimentAnalyzer();
        sentimentAnalyzer.initialize();
        SentimentResult sentimentResult = sentimentAnalyzer.getSentimentResult(text);

        System.out.println("TEXT: " + text);
        System.out.println("Sentiment Score: " + sentimentResult.getSentimentScore());
        System.out.println("Sentiment Type: " + sentimentResult.getSentimentType());
        System.out.println("Very positive: " + sentimentResult.getSentimentClass().getVeryPositive()+"%");
        System.out.println("Positive: " + sentimentResult.getSentimentClass().getPositive()+"%");
        System.out.println("Neutral: " + sentimentResult.getSentimentClass().getNeutral()+"%");
        System.out.println("Negative: " + sentimentResult.getSentimentClass().getNegative()+"%");
        System.out.println("Very negative: " + sentimentResult.getSentimentClass().getVeryNegative()+"%");

        return sentimentResult;
    }
    public static String getSentiment(SentimentResult sentimentResult) {
        String sentimentType = sentimentResult.getSentimentType();
        SentimentClassification sentimentClass = sentimentResult.getSentimentClass();
        String dump = "";
        double weight = Double.MIN_VALUE;
        switch(sentimentType){
            case "VeryPositive":
                weight = sentimentClass.getVeryPositive();
                break;
            case "Positive":
                weight = sentimentClass.getPositive();
                break;
            case "Neutral":
                weight = sentimentClass.getNeutral();
                break;
            case "Negative":
                weight = sentimentClass.getNegative();
                break;
            case "VeryNegative":
                weight = sentimentClass.getVeryNegative();
                break;
        }
        return String.format("%s;%3.2f%%",sentimentType, weight);
    }


    }
