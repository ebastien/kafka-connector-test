package name.ebastien.kafka.connect

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import com.typesafe.scalalogging.Logger

import collection.JavaConverters._

/**
 * TestSourceTask is an infinite source of records
 */
class TestSourceTask extends SourceTask {

  private val log = Logger[TestSourceTask]

  private var state = None : Option[TestSourceState]

  override def poll(): java.util.List[SourceRecord] = {

    log.debug("Waiting for the next event...")

    this.synchronized {
      wait(1000)
    }

    state.map(_.poll).getOrElse(List()).asJava
  }

  override def start(props: java.util.Map[String,String]): Unit = {

    val topic = Option(props.get(TestSourceConnector.topicParam))

    if (topic.isEmpty)
      throw new ConnectException(
        "TestSourceTask configuration missing topic setting"
      )

    state = topic.map(t => TestSourceState(t))

    log.info("Task started")
  }

  override def stop(): Unit = {
    log.info("Task stopped")
  }

  override def version(): String = TestSourceConnector.version
}
