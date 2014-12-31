import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.JavaConversions._
import scala.collection.mutable.Buffer
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.Regions
import com.amazonaws.services.sqs.AmazonSQS
import com.amazonaws.services.sqs.AmazonSQSClient
import com.amazonaws.services.sqs.model.CreateQueueRequest
import com.amazonaws.services.sqs.model.DeleteMessageRequest
import com.amazonaws.services.sqs.model.Message
import com.amazonaws.services.sqs.model.MessageAttributeValue
import com.amazonaws.services.sqs.model.ReceiveMessageRequest
import com.amazonaws.services.sqs.model.SendMessageRequest

object SqsSample {
  
  /**
   * To run this program, you need 2 keys
   * @param args (0): accessKeyId, (1): secretAccessKey
   * [FAQ]: http://aws.amazon.com/jp/sqs/faqs/
   */  
  def main(args: Array[String]): Unit = {
    val credentials: AWSCredentials = new BasicAWSCredentials(args(0), args(1))
    
    val sqs = new AmazonSQSClient(credentials)
    sqs.setRegion(Regions.US_WEST_2)
    
    val queueName = "myQueue"
    
    val queueUrl = this.createQueue(sqs, queueName)
    this.sendMsg(sqs, queueUrl)
    this.receiveMsg(sqs, queueUrl)
    
    println(queueUrl)
  }
  
  def createQueue(sqs: AmazonSQS, queueName: String): String = {
    if (sqs.listQueues(queueName).getQueueUrls().isEmpty()) {
      println("create")
      val createQueueRequest = new CreateQueueRequest(queueName)
      val createQueueResult = sqs.createQueue(createQueueRequest)
      createQueueResult.getQueueUrl
    } else {
      println("get")
      sqs.listQueues(queueName).getQueueUrls().get(0)
    }
  }
  
  def sendMsg(sqs: AmazonSQSClient, queueUrl: String) = {
    // [DataTypes]: http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/SQSMessageAttributes.html
    val sendMsgReq = new SendMessageRequest()
    .withQueueUrl(queueUrl)
    .withMessageAttributes(
        Map("key1" -> new MessageAttributeValue().withDataType("String").withStringValue("value1"), 
            "key2" -> new MessageAttributeValue().withDataType("String").withStringValue("value2")))
    .withMessageBody(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()))
    sqs.sendMessage(sendMsgReq)
  }
  
  def receiveMsg(sqs: AmazonSQSClient, queueUrl: String): Buffer[Message] = {
    val receiveMsgReq = new ReceiveMessageRequest()
    .withQueueUrl(queueUrl)
    .withMaxNumberOfMessages(10)
    .withWaitTimeSeconds(20)
    .withVisibilityTimeout(30) // クライアントがキューのメッセージをReceiveしたとき、指定した秒数分は他のクライアントがReceiveできない
    
    val receiveMsgResult = sqs.receiveMessage(receiveMsgReq)
    val msgList = new ListBuffer[Message]()
    receiveMsgResult.getMessages().foreach { x => 
      msgList += x
      println(x.getBody)
      this.deleteMsg(sqs, queueUrl, x.getReceiptHandle)
    }
    msgList
  }

  def deleteMsg(sqs: AmazonSQSClient, queueUrl: String, receiptHandle: String) = {
    val deleteMsgReq = new DeleteMessageRequest()
    .withQueueUrl(queueUrl)
    .withReceiptHandle(receiptHandle)
    sqs.deleteMessage(deleteMsgReq)
  }
}