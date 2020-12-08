package it.unicam.da.streaming.sentiment;

import com.stanford_nlp.model.SentimentResult;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Date;

public class SentimentModel implements Serializable {

    public String getSentiment() {
        return sentiment;
    }

    public void setSentiment(String sentiment) {
        this.sentiment = sentiment;
    }


    private String location;
    String sentiment;
    Date date;
    Double percentage;
    String text;
    String userMail;
    String userName;
    String userScreenName;

    public String getUserMail() {
        return userMail;
    }

    public void setUserMail(String userMail) {
        this.userMail = userMail;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getUserScreenName() {
        return userScreenName;
    }

    public void setUserScreenName(String userScreenName) {
        this.userScreenName = userScreenName;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public Double getPercentage() {
        return percentage;
    }

    public void setPercentage(Double percentage) {
        this.percentage = percentage;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public Double getLocationLat() {
        return locationLat;
    }

    public void setLocationLat(Double locationLat) {
        this.locationLat = locationLat;
    }

    public Double getLocationLong() {
        return locationLong;
    }

    public void setLocationLong(Double locationLong) {
        this.locationLong = locationLong;
    }

    Double locationLat;
    Double locationLong;

    public void setSentiment(SentimentModel sentimentModel) {
        this.setSentiment(sentimentModel.getSentiment());
        this.setPercentage(sentimentModel.getPercentage());
    }

    public void setSentiment(Sentiment sentiment) {
        Sentiment.SentimentResultModel sentimentModel = sentiment.getSentimentModel();
        setSentiment(sentimentModel.getSentimentType());
        setPercentage(sentimentModel.getPercentage());
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public void setUserLocation(String location) {
        this.location = location;
    }
}
