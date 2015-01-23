import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor

class KinesisSampleIRecordProcessorFactory extends IRecordProcessorFactory {
  
  def createProcessor(): IRecordProcessor = new KinesisSampleIRecordProcessor
  
}