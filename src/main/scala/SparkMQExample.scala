import java.util.Calendar

import com.ibm.spark.streaming.mq.MQConsumerFactory
import javax.jms._
import net.tbfe.spark.streaming.jms.JmsStreamUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel
import scala.concurrent.duration._

object SparkMQExample {

  def functionToCreateContext(host:String,port:String,qm:String,qn:String,channel:String,checkpointDir:String)= {
    val sparkConf = new SparkConf()
      .setAppName("MQJMSReceiver")
      .set("spark.streaming.receiver.writeAheadlog.enable", "true")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc,Seconds(10))
    ssc.checkpoint(checkpointDir)
    val user = ""
    val credentials = ""

    val converter: Message => Option[String]= {
      case msg: TextMessage => Some(msg.getText)
      case _ => None
    }
    val mqConsumerProps = MQConsumerFactory(host, port.toInt,qm,qn,user,credentials)
    val msgs =JmsStreamUtils.createSynchronousJmsQueueStream(ssc, mqConsumerProps, converter, 1000, 1.second, 10.seconds, StorageLevel.MEMORY_AND_DISK_SER_2)

    msgs.foreachRDD(rdd => {
      if(!rdd.partitions.isEmpty){
        println("messages received: ")
        rdd.foreach(println)
        rdd.saveAsTextFile("hdfs://..."+Calendar.getInstance().getTimeInMillis)
      } else{
        println("rdd is empty")
      }
    })
    ssc
  }
}
