package it.unicam.da.streaming.sentiment;

import com.stanford_nlp.model.SentimentResult;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.mqtt.MQTTUtils;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import twitter4j.FilterQuery;
import twitter4j.Status;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Hello world!
 *
 */
public class AppTwitter
{
    public static void main( String[] args )
    {


        // Set the system properties so that Twitter4j library used by Twitter stream
        // can use them to generate OAuth credentials
        // https://github.com/apache/bahir/blob/master/streaming-twitter/examples/src/main/java/org/apache/spark/examples/streaming/twitter/JavaTwitterHashTagJoinSentiments.java
        AppTwitter.initParams();
        System.out.println( "Hello World Twitter!" );

        SparkConf sparkConf = new SparkConf().setAppName("JavaTwitterHashTagJoinSentiments");
        sparkConf.setMaster("local[*]");


        // https://github.com/apache/bahir/blob/master/streaming-mqtt/examples/src/main/scala/org/apache/spark/examples/streaming/mqtt/MQTTWordCount.scala
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(5000));
        jssc.sparkContext().setLogLevel("ERROR");

        FilterQuery filters = new FilterQuery()
                .language("en")
                .track("covid");
        JavaReceiverInputDStream<Status> stream = TwitterUtils.createFilteredStream(jssc, null, filters, StorageLevel.MEMORY_AND_DISK_SER_2());
        //JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jssc);//, new String[]{});



        //JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jssc);//, new String[]{});
        JavaDStream<String> words = stream.flatMap(new FlatMapFunction<Status, String>() {
            @Override
            public Iterator<String> call(Status s) {
                return Arrays.asList(s.getText().split(" ")).iterator();
            }
        });

        JavaDStream<String> hashTags = words.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String word) {
                return word.startsWith("#");
            }
        });

        // Read in the word-sentiment list and create a static RDD from it
        String wordSentimentFilePath = "AFINN-111.txt";
        final JavaPairRDD<String, Double> wordSentiments = jssc.sparkContext()
                .textFile(wordSentimentFilePath)
                .mapToPair(new PairFunction<String, String, Double>(){
                    @Override
                    public Tuple2<String, Double> call(String line) {
                        String[] columns = line.split("\t");
                        return new Tuple2<>(columns[0], Double.parseDouble(columns[1]));
                    }
                });

        JavaPairDStream<String, Integer> hashTagCount = hashTags.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) {
                        // leave out the # character
                        return new Tuple2<>(s.substring(1), 1);
                    }
                });

        JavaPairDStream<String, Integer> hashTagTotals = hashTagCount.reduceByKeyAndWindow(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer a, Integer b) {
                        return a + b;
                    }
                }, new Duration(10000));

        // Determine the hash tags with the highest sentiment values by joining the streaming RDD
        // with the static RDD inside the transform() method and then multiplying
        // the frequency of the hash tag by its sentiment value
        JavaPairDStream<String, Tuple2<Double, Integer>> joinedTuples =
                hashTagTotals.transformToPair(new Function<JavaPairRDD<String, Integer>,
                        JavaPairRDD<String, Tuple2<Double, Integer>>>() {
                    @Override
                    public JavaPairRDD<String, Tuple2<Double, Integer>> call(
                            JavaPairRDD<String, Integer> topicCount) {
                        return wordSentiments.join(topicCount);
                    }
                });

        JavaPairDStream<String, Double> topicHappiness = joinedTuples.mapToPair(
                new PairFunction<Tuple2<String, Tuple2<Double, Integer>>, String, Double>() {
                    @Override
                    public Tuple2<String, Double> call(Tuple2<String,
                            Tuple2<Double, Integer>> topicAndTuplePair) {
                        Tuple2<Double, Integer> happinessAndCount = topicAndTuplePair._2();
                        return new Tuple2<>(topicAndTuplePair._1(),
                                happinessAndCount._1() * happinessAndCount._2());
                    }
                });

        JavaPairDStream<Double, String> happinessTopicPairs = topicHappiness.mapToPair(
                new PairFunction<Tuple2<String, Double>, Double, String>() {
                    @Override
                    public Tuple2<Double, String> call(Tuple2<String, Double> topicHappiness) {
                        return new Tuple2<>(topicHappiness._2(),
                                topicHappiness._1());
                    }
                });

        JavaPairDStream<Double, String> happiest10 = happinessTopicPairs.transformToPair(
                new Function<JavaPairRDD<Double, String>, JavaPairRDD<Double, String>>() {
                    @Override
                    public JavaPairRDD<Double, String> call(
                            JavaPairRDD<Double, String> happinessAndTopics) {
                        return happinessAndTopics.sortByKey(false);
                    }
                }
        );

        // Print hash tags with the most positive sentiment values
        happiest10.foreachRDD(new VoidFunction<JavaPairRDD<Double, String>>() {
            @Override
            public void call(JavaPairRDD<Double, String> happinessTopicPairs) {
                List<Tuple2<Double, String>> topList = happinessTopicPairs.take(10);
                System.out.println(
                        String.format("\nHappiest topics in last 10 seconds (%s total):",
                                happinessTopicPairs.count()));
                for (Tuple2<Double, String> pair : topList) {
                    System.out.println(
                            String.format("%s (%s happiness)", pair._2(), pair._1()));
                }
            }
        });


        try {

            jssc.start();              // Start the computation
            jssc.awaitTermination();   // Wait for the computation to terminate
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void initParams(){
        System.setProperty("twitter4j.oauth.consumerKey", System.getProperty("consumerKey", "a"));
        System.setProperty("twitter4j.oauth.consumerSecret", System.getProperty("consumerSecret", "b"));
        System.setProperty("twitter4j.oauth.accessToken", System.getProperty("accessToken", "c"));
        System.setProperty("twitter4j.oauth.accessTokenSecret", System.getProperty("accessTokenSecret", "ds"));

        System.out.println("*** TWITTER ACCESS Information");
        System.out.println("twitter4j.oauth.consumerKey           "+ System.getProperty("consumerKey", ""));
        System.out.println("twitter4j.oauth.consumerSecret        "+ System.getProperty("consumerSecret", ""));
        System.out.println("twitter4j.oauth.accessToken           "+ System.getProperty("accessToken", ""));
        System.out.println("twitter4j.oauth.accessTokenSecret     "+ System.getProperty("accessTokenSecret", ""));
    }
}
