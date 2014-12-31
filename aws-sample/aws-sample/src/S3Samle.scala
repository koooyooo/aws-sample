import java.io.File
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.s3.model.GetObjectRequest
import com.amazonaws.services.s3.model.S3Object
import org.apache.commons.io.IOUtils
import java.io.FileOutputStream

object S3Samle {
  
  def main(args: Array[String]): Unit = {
    val credentials: AWSCredentials = new BasicAWSCredentials(args(0), args(1))
    
    val s3: AmazonS3 = new AmazonS3Client(credentials)
    .withRegion(Regions.US_WEST_2)
    
    val bucketName = "org.onda.bucket.sample"
    val keyName = "death_star2.jpg"
    
    this.putObject(s3, bucketName, keyName, new File("C:/Users/koyo/Desktop/death_star2.jpg"))
    val getObject = this.getObject(s3, bucketName, keyName, new File("C:/Users/koyo/Desktop/death_star3.jpg"))
  }

  def putObject(s3: AmazonS3, bucketName: String, keyName: String, file: File) = {
    val putObjectRequest = new PutObjectRequest(bucketName, keyName, file)
    s3.putObject(putObjectRequest)
  }
  
  def getObject(s3: AmazonS3, bucketName: String, key: String, outputFile: File) = {
    val getObjectRequest = new GetObjectRequest(bucketName, key)
    val s3Object = s3.getObject(getObjectRequest)
    IOUtils.copy(s3Object.getObjectContent, new FileOutputStream(outputFile))
  }
}