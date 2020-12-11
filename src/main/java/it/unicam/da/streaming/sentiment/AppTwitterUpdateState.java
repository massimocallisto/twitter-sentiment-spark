package it.unicam.da.streaming.sentiment;

import com.mongodb.spark.MongoSpark;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import twitter4j.FilterQuery;
import twitter4j.Status;
import twitter4j.User;

import java.util.Arrays;
import java.util.List;

/**
 * Hello world!
 *
 */
public class AppTwitterUpdateState
{
    public static void main( String[] args )
    {


        // Set the system properties so that Twitter4j library used by Twitter stream
        // can use them to generate OAuth credentials
        // https://github.com/apache/bahir/blob/master/streaming-twitter/examples/src/main/java/org/apache/spark/examples/streaming/twitter/JavaTwitterHashTagJoinSentiments.java
        System.setProperty("twitter4j.oauth.consumerKey", "cWV5fXGslcBph3tHHT52QaRGe");
        System.setProperty("twitter4j.oauth.consumerSecret", "z9r1YXRHJvze8e0oXwGiE2Oi6T6v9PcIZgDx4qqTb3cqwm3vsB");
        System.setProperty("twitter4j.oauth.accessToken", "11067472-Br433xWyFySLuMNyZQfC1oapOBVxlrHRgC864L8ef");
        System.setProperty("twitter4j.oauth.accessTokenSecret", "Z2gst5zkU0r4Mddn6uaEt8hn0PrHoWZsJKgMuXMEPUG4O");

        System.out.println( "Hello World Twitter!" );
        SparkConf sparkConf = new SparkConf()
                .setAppName("JavaTwitterHashTagJoinSentiments")
                .setAppName("JavaTwitterHashTagJoinSentiments");
        sparkConf.setMaster("local[2]");



        // https://github.com/apache/bahir/blob/master/streaming-mqtt/examples/src/main/scala/org/apache/spark/examples/streaming/mqtt/MQTTWordCount.scala
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(5000));
        jssc.sparkContext().setLogLevel("ERROR");

        /*
            NTE: checkpoint is now needed to persist the state
         */
        jssc.checkpoint("c:/tmp/state");

        FilterQuery filters = new FilterQuery()
                .language("en")
                .track("covid");
        JavaReceiverInputDStream<Status> stream = TwitterUtils.createFilteredStream(jssc, null, filters, StorageLevel.MEMORY_AND_DISK_SER_2());
        //JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jssc);//, new String[]{});

        JavaDStream<SentimentModel> sentimentTweet = stream.map(new Function<Status, SentimentModel>() {
            @Override
            public SentimentModel call(Status status) throws Exception {
                SentimentModel sm = new SentimentModel();
                sm.setText(status.getText());
                sm.setDate(status.getCreatedAt());
                Sentiment sentiment = new Sentiment(sm.getText());
                sm.setSentiment(sentiment);

                User user = status.getUser();
                sm.setUserMail(user.getEmail());
                sm.setUserName(user.getName());
                sm.setUserScreenName(user.getScreenName());
                sm.setUserLocation(user.getLocation());

                return sm;
            }
        }).filter(new Function<SentimentModel, Boolean>() {
            @Override
            public Boolean call(SentimentModel s) throws Exception {
                return s.getText().length() > 20;
            }
        });

        JavaPairDStream<String, Integer> mapToPair = sentimentTweet.mapToPair(new PairFunction<SentimentModel, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(SentimentModel sentimentModel) throws Exception {
                return new Tuple2<>(sentimentModel.getSentiment(), 1);
            }
        });

        // https://github.com/teamclairvoyant/spark-streaming-workshop/blob/master/src/main/java/com/clairvoyant/spark162/examples/UpdateStateByKey.java
        Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction =
                new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
                    public Optional<Integer> call(List<Integer> values, Optional<Integer> state) {
                        Integer newSum = state.or(0);

                        //         System.out.println(values);
                        for(int i : values)
                        {
                            newSum += i;
                        }
                        return Optional.of(newSum);
                    }
                };

        JavaPairDStream<String, Integer> runningCounts =
                mapToPair.updateStateByKey(updateFunction);
        runningCounts.print();

        try {

            jssc.start();              // Start the computation
            jssc.awaitTermination();   // Wait for the computation to terminate
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
