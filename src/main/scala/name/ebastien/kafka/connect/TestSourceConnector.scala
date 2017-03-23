package name.ebastien.kafka.connect

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;

import com.typesafe.scalalogging.Logger

import collection.JavaConverters._

/**
 * TestSourceConnector is an infinite source of records
 */
class TestSourceConnector extends SourceConnector {

  private val log = Logger[TestSourceConnector]

  private var connectorProps = new java.util.HashMap()
                             : java.util.Map[String,String]

  override def config(): ConfigDef = TestSourceConnector.configDef
  
  override def start(props: java.util.Map[String,String]): Unit = {

    val topic = Option(props.get(TestSourceConnector.topicParam))

    if (topic.isEmpty)
      throw new ConnectException(
        "TestSourceConnector configuration missing topic setting"
      )

    connectorProps = props

    log.info("Connector started")
  }
  
  override def stop(): Unit = {
    log.info("Connector stopped")
  }
  
  override def taskClass(): Class[_ <: Task] = classOf[TestSourceTask]

  override def taskConfigs(maxTasks: Int)
    : java.util.List[java.util.Map[String,String]] = {

    List.fill(maxTasks)(connectorProps).asJava
  }
  
  override def version(): String = TestSourceConnector.version
}

object TestSourceConnector {

  val topicParam = "topic"

  val configDef = new ConfigDef()
                    .define(topicParam, Type.STRING, Importance.HIGH,
                      "The topic to publish data to")

  def version : String = AppInfoParser.getVersion
}
