package it.unicam.da.streaming.sentiment;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import twitter4j.FilterQuery;
import twitter4j.Status;
import twitter4j.User;

import java.util.List;

/**
 * Hello world!
 *
 */
public class AppTwitterReduceKeyWindow
{
    public static void main( String[] args )
    {


        // Set the system properties so that Twitter4j library used by Twitter stream
        // can use them to generate OAuth credentials
        // https://github.com/apache/bahir/blob/master/streaming-twitter/examples/src/main/java/org/apache/spark/examples/streaming/twitter/JavaTwitterHashTagJoinSentiments.java

        AppTwitter.initParams();
        System.out.println( "Hello World Twitter!" );

        SparkConf sparkConf = new SparkConf()
                .setAppName("JavaTwitterHashTagJoinSentiments")
                .setAppName("JavaTwitterHashTagJoinSentiments");
        sparkConf.setMaster("local[2]");



        // https://github.com/apache/bahir/blob/master/streaming-mqtt/examples/src/main/scala/org/apache/spark/examples/streaming/mqtt/MQTTWordCount.scala
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(1000));
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
        // Reduce function adding two integers, defined separately for clarity
        Function2<Integer, Integer, Integer> reduceFunc = new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        };
        // https://stackoverflow.com/questions/49042195/spark-streaming-reducebykeyandwindow-example
        Duration window = Durations.seconds(10);
        Duration sliding = Durations.seconds(3);
        JavaPairDStream<String, Integer> windowedWordCounts = mapToPair.reduceByKeyAndWindow(reduceFunc, window, sliding);
        windowedWordCounts.print();


        try {

            jssc.start();              // Start the computation
            jssc.awaitTermination();   // Wait for the computation to terminate
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
