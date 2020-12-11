package it.unicam.da.streaming.sentiment;

import com.mongodb.spark.MongoSpark;
import com.stanford_nlp.model.SentimentResult;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import twitter4j.FilterQuery;
import twitter4j.Status;
import twitter4j.User;
import twitter4j.auth.Authorization;
import twitter4j.auth.BasicAuthorization;
import twitter4j.auth.NullAuthorization;
import twitter4j.auth.OAuth2Authorization;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Hello world!
 *
 */
public class AppTwitter2
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
                .set("spark.mongodb.input.uri", "mongodb://192.168.17.85/local")
                .set("spark.mongodb.output.uri", "mongodb://192.168.17.85/local")
                .setAppName("JavaTwitterHashTagJoinSentiments");
        sparkConf.setMaster("local[2]");


        // https://github.com/apache/bahir/blob/master/streaming-mqtt/examples/src/main/scala/org/apache/spark/examples/streaming/mqtt/MQTTWordCount.scala
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(5000));
        jssc.sparkContext().setLogLevel("ERROR");

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
        sentimentTweet.foreachRDD(new VoidFunction<JavaRDD<SentimentModel>>() {
            @Override
            public void call(JavaRDD<SentimentModel> stringJavaRDD) throws Exception {
                //JavaRDD<SentimentModel> rddModels = stringJavaRDD.map(new TwitterMapClass());
                SQLContext sql = new SQLContext(jssc.sparkContext());
                Dataset<Row> sentimentDF = sql.createDataFrame(stringJavaRDD, SentimentModel.class);
                sentimentDF.printSchema();
                sentimentDF.show();
                MongoSpark.write(sentimentDF).option("collection", "sentiment").mode("append").save();

            }
        });

        try {

            jssc.start();              // Start the computation
            jssc.awaitTermination();   // Wait for the computation to terminate
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
