package it.unicam.da.streaming.sentiment;

import com.stanford_nlp.model.SentimentResult;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.mqtt.MQTTUtils;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Hello world!
 *
 */
public class AppMongo
{
    public static void main( String[] args )
    {

        System.out.println( "Sentiment to Mongo!" );
        SparkConf sparkConf = new SparkConf().setAppName("JavaTwitterHashTagJoinSentiments");
        sparkConf.setMaster("local[2]");


        // https://github.com/apache/bahir/blob/master/streaming-mqtt/examples/src/main/scala/org/apache/spark/examples/streaming/mqtt/MQTTWordCount.scala
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(10000));
        final JavaReceiverInputDStream<String> stream = MQTTUtils.createStream(
                jssc,
                "tcp://bthermalappliance.westeurope.cloudapp.azure.com:1883",
                "#",
                StorageLevel.MEMORY_ONLY_2(),
                "user",
                "user");

        JavaPairDStream<String, Integer> js = stream
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
        JavaDStream<String> map = stream.map(s -> {
            String text = s; // TODO: if jason parsing is needed
            SentimentResult result = new Sentiment().analyze(text);
            System.out.println("Sentiment Score: " + result.getSentimentScore());
            System.out.println("Sentiment Type: " + result.getSentimentType());
            return Sentiment.getSentiment(result)+";"+text;
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
