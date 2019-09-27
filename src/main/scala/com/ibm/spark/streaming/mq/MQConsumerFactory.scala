package com.ibm.spark.streaming.mq

import com.ibm.mq.jms._
import javax.jms._
import org.apache.spark.Logging
import org.apache.spark.streaming.jms.MessageConsumerFactory

/**
  *
  * @param mqHost                  MQ hostname
  * @param mqPort                  MQ port number
  * @param mqQmgr                  MQ queue manager name
  * @param mqQname                 MQ queue name / topic
  * @param mqUser                 User name for connecting to MQ Queue
  * @param mqCred                 Credentials for the specified user
  * @param connectionFactoryName Name of connection factory configured in JNDI
  * @param messageSelector       Message selector. Use Empty string for no message filter
  */


case class MQConsumerFactory(mqHost: String,
                             mqPort: Int,
                             mqQmgr: String,
                             mqQname: String,
                             mqUser: String,
                             mqCred: String,
                             mqChannel: String,
                             connectionFactoryName: String = "ConnectionFactory",
                             messageSelector: String = "")
  extends MessageConsumerFactory with Logging {

  @volatile
  @transient
  var host: String = _
  var port: Int = _
  var qmgr: String = _
  var qname: String = _
  var uname: String = _
  var creds: String = _
  var channel: String = _

  override def makeConsumer(session: Session): MessageConsumer = {
    var queue = new MQQueue()
    queue = session.createQueue(qname).asInstanceOf[MQQueue]
    session.createConsumer(queue, messageSelector)
  }

  override def makeConnection: Connection = {
    if (host == null) {
      host = mqHost
      port = mqPort
      qmgr = mqQmgr
      qname = mqQname
      uname = mqUser
      creds = mqCred
      channel = mqChannel
    }
    val conFactory = new MQQueueConnectionFactory()
    conFactory.setHostName(host)
    conFactory.setPort(port)
    conFactory.setTransportType(JMSC.MQJMS_TP_CLIENT_MQ_TCPIP)
    conFactory.setQueueManager(qmgr)
    conFactory.setChannel(channel)

    val qCon = conFactory.createQueueConnection(uname, creds).asInstanceOf[MQQueueConnection]
    qCon
  }
}
