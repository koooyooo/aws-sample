
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.Regions
import com.amazonaws.services.kinesis.model.CreateStreamRequest
import com.amazonaws.services.kinesis.model.PutRecordResult
import com.amazonaws.services.kinesis.model.PutRecordRequest
import java.nio.ByteBuffer

object KinesisSample {
  
  def main(args: Array[String]): Unit = {
    val streamName = "myStream"
    val kinesis = this.init(args(0), args(1))
    this.writeRecords(kinesis, streamName)
  }
  
  def init(accessKeyId: String, secretAccessKey: String): AmazonKinesisClient = {
    val credentials: AWSCredentials = new BasicAWSCredentials(accessKeyId, secretAccessKey)
    val kinesis = new AmazonKinesisClient(credentials)
    kinesis.setRegion(Regions.US_WEST_2)
    kinesis
  }
  
  def createStream(kinesis: AmazonKinesisClient): Unit = {
    val createStreamRequest = new CreateStreamRequest
    
  }
  
  def writeRecords(kinesis: AmazonKinesisClient, streamName: String): Unit = {
    for (i <- 1 to 10) {
      val putRecordRequest = new PutRecordRequest();
      putRecordRequest.setStreamName(streamName);
      putRecordRequest.setData(ByteBuffer.wrap(i.toString.getBytes));
      putRecordRequest.setPartitionKey(String.format("partitionKey-%d", i.asInstanceOf[Object]));
      val putRecordResult = kinesis.putRecord(putRecordRequest);
      
      println("Successfully putrecord, partition key : " + putRecordRequest.getPartitionKey()
              + ", ShardID : " + putRecordResult.getShardId());
    }
    
  }
  
}