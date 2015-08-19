import org.apache.spark
import org.apache.spark.SparkContext._
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

class MyRegistrator extends KryoRegistrator {
    override def registerClasses(kryo: Kryo) {
    }
}

/**
 * Created by gaoyunxiang on 27/8/14.
 */
object SparkCommon {

    val conf = new spark.SparkConf().setAppName("ImageTextModelMerge@gaoyunxiang")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "MyRegistrator")
    conf.set("spark.kryoserializer.buffer.max.mb", "512")
    conf.set("spark.driver.maxResultSize", "8g")
    conf.set("spark.akka.frameSize", "500")
    conf.set("spark.akka.askTimeout", "1000")
    conf.set("spark.task.maxFailures", "40")
    conf.set("spark.speculation", "true")
    //    conf.set("spark.shuffle.consolidateFiles", "true")
//    conf.set("spark.shuffle.spill", "false")
//    conf.set("spark.eventLog.enabled", "true")
    //    conf.set("spark.default.parallelism", "300")

    val sc = new spark.SparkContext(conf)


}

