import org.apache.spark.streaming.StreamingContext


object Main{
  def main(args: Array[String]): Unit = {
    if (args.length < 4){
      System.err.println("Usage: <host> <port> <queue manager> <queue name>")
      System.exit(1)
    }

    val Array(host, port, qm, qn)=args

    val chechpointDir="hdfs:///..."

    val streamContext = StreamingContext.getOrCreate(chechpointDir, ()=> SparkMQExample.functionToCreateContext(host,port,qm,qn,"",chechpointDir))

    streamContext.start()
    streamContext.awaitTermination()
  }
}