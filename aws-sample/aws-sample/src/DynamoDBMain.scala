// import scala.collection.JavaConversions._
import scala.collection.JavaConversions._
import scala.collection.mutable.Map
import scala.collection.mutable.Set
import scala.collection.mutable.ListBuffer
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator
import com.amazonaws.services.dynamodbv2.model.Condition
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest
import com.amazonaws.services.dynamodbv2.model.GetItemRequest
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement
import com.amazonaws.services.dynamodbv2.model.KeyType
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex
import com.amazonaws.services.dynamodbv2.model.Projection
import com.amazonaws.services.dynamodbv2.model.ProjectionType
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput
import com.amazonaws.services.dynamodbv2.model.PutItemRequest
import com.amazonaws.services.dynamodbv2.model.QueryRequest
import com.amazonaws.services.dynamodbv2.model.ScanRequest
import com.amazonaws.services.dynamodbv2.util.Tables
import com.amazonaws.services.dynamodbv2.model.GetItemResult
import com.amazonaws.services.dynamodbv2.model.QueryResult
import com.amazonaws.services.dynamodbv2.model.ScanResult


object DynamoDBMain {
  
  /**
   * To run this program, you need 2 keys
   * @param args (0): accessKeyId, (1): secretAccessKey
   */
  def main(args: Array[String]): Unit = {
    
    var dynamoDB = this.initDB(args(0), args(1))
    val tableName = "my_first_table"
    if (!Tables.doesTableExist(dynamoDB, tableName)) {
      this.createTable(dynamoDB, tableName)
    }
    this.addItem(dynamoDB, tableName)
    this.addItemByMapper(dynamoDB, tableName)
    this.getItem(dynamoDB, tableName)
    this.queryItems(dynamoDB, tableName)
    this.scanItems(dynamoDB, tableName)
  }
  
  
  def initDB(accessKeyId: String, secretAccessKey: String): AmazonDynamoDBClient = {
    val credentials: AWSCredentials = new BasicAWSCredentials(accessKeyId, secretAccessKey)
    val dynamoDB = new AmazonDynamoDBClient(credentials)
    dynamoDB.setRegion(Regions.US_WEST_2)
    dynamoDB
  }
  
  
  def createTable(dynamoDB: AmazonDynamoDBClient, tableName: String) = {
    val createTableRequest = new CreateTableRequest()
    .withTableName(tableName)
    .withKeySchema(
        new KeySchemaElement().withAttributeName("id").withKeyType(KeyType.HASH),
        new KeySchemaElement().withAttributeName("range").withKeyType(KeyType.RANGE))
    .withAttributeDefinitions(
        new AttributeDefinition().withAttributeName("id").withAttributeType("S"),
        new AttributeDefinition().withAttributeName("range").withAttributeType("S"),
        new AttributeDefinition().withAttributeName("range2").withAttributeType("S"))
    .withProvisionedThroughput(
        new ProvisionedThroughput()
        .withReadCapacityUnits(1L)
        .withWriteCapacityUnits(1L))
    
    createTableRequest.setLocalSecondaryIndexes(Set(this.createLocalSecondaryIndex(dynamoDB, tableName, "ind-" + tableName)))
        
    dynamoDB.createTable(createTableRequest)
    Tables.waitForTableToBecomeActive(dynamoDB, tableName);
    
    println("create!")
  }
  
  
  def createLocalSecondaryIndex(dynamoDB: AmazonDynamoDBClient, tableName: String, indexName: String): LocalSecondaryIndex = {
    val projection = new Projection()
    .withProjectionType(ProjectionType.INCLUDE)
    .withNonKeyAttributes(Set("other"))
    
    val localSecondaryIndex = new LocalSecondaryIndex()
    .withIndexName(indexName)
    .withKeySchema(
        new KeySchemaElement().withAttributeName("id").withKeyType(KeyType.HASH),
        new KeySchemaElement().withAttributeName("range2").withKeyType(KeyType.RANGE))
    .withProjection(projection)
    localSecondaryIndex
  }
  
  
  def addItem(dynamoDB: AmazonDynamoDBClient, tableName: String) = {
    val item = Map("id" -> new AttributeValue().withS("hello world"),
                   "range" -> new AttributeValue().withS("new value"),
                   "range2" -> new AttributeValue().withS("new2 value"),
                   "other" -> new AttributeValue().withS("other value"))
    dynamoDB.putItem(new PutItemRequest(tableName, item))
  }
  
  
  def addItemByMapper(dynamoDB: AmazonDynamoDBClient, tableName: String) = {
    val item = new MyItem("hello world2", "new value2", "new value22", "other value2")
    new DynamoDBMapper(dynamoDB).save(item)
  }
  
  
  def getItem(dynamoDB: AmazonDynamoDBClient, tableName: String): GetItemResult = {
    val getItemRequest = new GetItemRequest()
    .withTableName(tableName)
    .withKey(Map("id" -> new AttributeValue().withS("hello world"), "range" -> new AttributeValue().withS("new value")))
    .withAttributesToGet("id", "range")
    val result = dynamoDB.getItem(getItemRequest)
    println("get: " + result)
    result
  }
  
  
  def queryItems(dynamoDB: AmazonDynamoDBClient, tableName: String): QueryResult = {
    val hashKeyCondition = new Condition()
    .withComparisonOperator(ComparisonOperator.EQ)
    .withAttributeValueList(new AttributeValue().withS("hello world"))
    
    val rangeKeyCondition = new Condition()
    .withComparisonOperator(ComparisonOperator.EQ)
    .withAttributeValueList(new AttributeValue().withS("new value"))
    
    val queryRequest = new QueryRequest()
    .withTableName(tableName)
    .withKeyConditions(Map("id" -> hashKeyCondition, "range" -> rangeKeyCondition))
    .withAttributesToGet("id", "range")
    
    val result = dynamoDB.query(queryRequest)
    println("query: " + result)
    result
  }
  
  
  def scanItems(dynamoDB: AmazonDynamoDBClient, tableName: String): ScanResult = {
    
     val hashKeyCondition = new Condition()
    .withComparisonOperator(ComparisonOperator.BEGINS_WITH)
    .withAttributeValueList(new AttributeValue().withS("hello world"))
    
    val otherCondition = new Condition()
    .withComparisonOperator(ComparisonOperator.BEGINS_WITH)
    .withAttributeValueList(new AttributeValue().withS("other value"))
    
    val scanRequest = new ScanRequest()
    .withTableName(tableName)
    .withScanFilter(Map("id" -> hashKeyCondition, "other" -> otherCondition))

    .withAttributesToGet("id", "range", "other")
    
    val result = dynamoDB.scan(scanRequest)
    println("scan: " + result)
    result
  }
  
}

@DynamoDBTable(tableName = "my_first_table")
case class MyItem(var id: String, var range: String, var range2: String, var other: String) {
  
  @DynamoDBHashKey(attributeName = "id")
  def getId() = this.id
  @DynamoDBRangeKey(attributeName = "range")
  def getRange() = this.range
  @DynamoDBAttribute(attributeName = "range2")
  def getRange2() = this.range2
  @DynamoDBAttribute(attributeName = "other")
  def getOther() = this.other
  
  def setId(id: String) = this.id = id
  def setRange(range: String) = this.range = range
  def setRange2(range2: String) = this.range2 = range2
  def setOther(other: String) = this.other = other
  
}
