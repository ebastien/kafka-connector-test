package name.ebastien.kafka.connect

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import collection.JavaConverters._

class TestSourceTask extends SourceTask {

  var state : Option[TestSourceState] = None

  override def poll(): java.util.List[SourceRecord] = {

    wait(1000)

    state.map(_.poll).getOrElse(List()).asJava
  }

  override def start(props: java.util.Map[String,String]): Unit = {

    val topic = Option(props.get(TestSourceConnector.topicParam))

    if (topic.isEmpty)
      throw new ConnectException(
        "TestSourceTask configuration missing topic setting"
      )

    state = topic.map(t => TestSourceState(t))
  }

  override def stop(): Unit = {}

  override def version(): String = TestSourceConnector.version
}
