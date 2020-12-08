package it.unicam.da.streaming.sentiment;

import com.stanford_nlp.model.SentimentResult;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.ReceiverInputDStream;
import org.apache.spark.streaming.mqtt.MQTTUtils;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Function1;
import scala.Tuple2;
import scala.collection.TraversableOnce;
import twitter4j.Status;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {

        System.out.println( "Hello World!" );
        SparkConf sparkConf = new SparkConf().setAppName("JavaTwitterHashTagJoinSentiments");
        sparkConf.setMaster("local[2]");


        // https://github.com/apache/bahir/blob/master/streaming-mqtt/examples/src/main/scala/org/apache/spark/examples/streaming/mqtt/MQTTWordCount.scala
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(5000));
        final JavaReceiverInputDStream<String> stream = MQTTUtils.createStream(
                jssc,
                "tcp://test.mosquitto.org:1883", //"tcp://bthermalappliance.westeurope.cloudapp.azure.com:1883",
                "#",
                StorageLevel.MEMORY_ONLY_2()/*,
                "user",
                "user"*/
                );

        JavaDStream<String> map = stream.map(s -> {
            String text = s; // TODO: if jason parsing is needed
            Sentiment sentiment = new Sentiment(text);
            Sentiment.SentimentResultModel sentimentModel = sentiment.getSentimentModel();
            System.out.println("Sentiment Score: " + sentimentModel.getPercentage());
            System.out.println("Sentiment Type: " + sentimentModel.getSentimentType());
            return sentiment.getSentiment()+";"+text;
        });
        map.print();

       // js.print();

        try {

            jssc.start();              // Start the computation
            jssc.awaitTermination();   // Wait for the computation to terminate
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
