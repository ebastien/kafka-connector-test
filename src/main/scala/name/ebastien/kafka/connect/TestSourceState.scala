package name.ebastien.kafka.connect

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.data.Schema;

/**
 * TestSourceState is an infinite source of records
 */
case class TestSourceState(topic: String) {
  
  def poll : Seq[SourceRecord] = {

    val x = scala.util.Random.nextDouble()
    val y = 2.0 + 5.0 * x
    val sample = Array(y,x).mkString(",")

    val sourcePartition = new java.util.HashMap[String, String]()
    val sourceOffset = new java.util.HashMap[String, Long]()

    val record = new SourceRecord(
      sourcePartition,              // sourcePartition
      sourceOffset,                 // sourceOffset
      topic,                        // topic
      null,                         // partition
      null,                         // keySchema
      null,                         // key
      Schema.STRING_SCHEMA,         // valueSchema
      sample,                       // value
      System.currentTimeMillis()    // timestamp
    )

    List(record)
  }
}
