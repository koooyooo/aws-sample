import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.model.Record
import java.util.List
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason
import scala.collection.JavaConversions._

/**
 * http://mvnrepository.com/artifact/com.amazonaws/amazon-kinesis-client/1.2.0
 */
class KinesisSampleIRecordProcessor extends IRecordProcessor {
  
  def initialize(shardName: String): Unit = {
    println("initialize.." + shardName )
  }
  
  def processRecords(records: List[Record], checkPointer: IRecordProcessorCheckpointer): Unit = {
    println("process records")
    for (record <- records) { 
      println(record)
    }
  }
  
  def shutdown(checkPointer: IRecordProcessorCheckpointer, reason: ShutdownReason): Unit = {
    println("shutdown")
  }
  
}