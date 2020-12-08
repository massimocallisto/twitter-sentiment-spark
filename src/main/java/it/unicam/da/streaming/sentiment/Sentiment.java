package it.unicam.da.streaming.sentiment;


import com.stanford_nlp.SentimentAnalyzer.SentimentAnalyzer;
import com.stanford_nlp.model.SentimentClassification;
import com.stanford_nlp.model.SentimentResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

public class Sentiment {
    SentimentResult sentimentResult;

    public Sentiment(String text) {
        this.analyze(text);
    }

    public class SentimentResultModel{
        String sentimentType;
        Double percentage;

        public String getSentimentType() {
            return sentimentType;
        }

        public void setSentimentType(String sentimentType) {
            this.sentimentType = sentimentType;
        }

        public Double getPercentage() {
            return percentage;
        }

        public void setPercentage(Double percentage) {
            this.percentage = percentage;
        }
    }


    public void analyze(String text) {
        SentimentAnalyzer sentimentAnalyzer = new SentimentAnalyzer();
        sentimentAnalyzer.initialize();
        this.sentimentResult = sentimentAnalyzer.getSentimentResult(text);
    }
    public String getSentiment() {
        SentimentResultModel sentimentModel = getSentimentModel();
        return String.format("%s;%3.2f%%",sentimentModel.getSentimentType(), sentimentModel.getPercentage());
    }

    public SentimentResultModel getSentimentModel() {
        String sentimentType = sentimentResult.getSentimentType();
        SentimentClassification sentimentClass = sentimentResult.getSentimentClass();

        SentimentResultModel sm = new SentimentResultModel();
        sm.setSentimentType(sentimentType);

        double weight = Double.MIN_VALUE;
        switch(sentimentType){
            case "VeryPositive":
                weight = sentimentClass.getVeryPositive();
                sm.setPercentage(weight);
                break;
            case "Positive":
                weight = sentimentClass.getPositive();
                sm.setPercentage(weight);
                break;
            case "Neutral":
                weight = sentimentClass.getNeutral();
                sm.setPercentage(weight);
                break;
            case "Negative":
                weight = sentimentClass.getNegative();
                sm.setPercentage(weight);
                break;
            case "VeryNegative":
                weight = sentimentClass.getVeryNegative();
                sm.setPercentage(weight);
                break;
        }

        return sm;
    }


    }
