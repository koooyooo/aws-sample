import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker
import com.amazonaws.regions.Regions

object KinesisSampleClient {
  
  def main(args: Array[String]): Unit = {
    
    val credentials: AWSCredentials = new BasicAWSCredentials(args(0), args(1))
    val credentialsProvider = new AWSCredentialsProvider {
      def getCredentials = credentials
      def refresh: Unit = {}
    }
    
    val kinesisClientLibConfiguration = 
      new KinesisClientLibConfiguration("applicationName", "myStream", credentialsProvider, "workerId")
      .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)
      .withRegionName(Regions.US_WEST_2.getName)
    
    
    val worker = new Worker(new KinesisSampleIRecordProcessorFactory, kinesisClientLibConfiguration);
    
    worker.run()
  }
  
  
}