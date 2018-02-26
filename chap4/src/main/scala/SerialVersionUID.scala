import java.io.File
import java.util.UUID

import akka.actor.SupervisorStrategy.{Resume, Stop}
import akka.actor.{Actor, ActorLogging, OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated}

@SerialVersionUID(1L)
class DiskError(msg: String)
  extends Error(msg) with Serializable

@SerialVersionUID(1L)
class CorruptedFileException(msg: String, val file: File)
  extends Exception(msg) with Serializable

@SerialVersionUID(1L)
class DbNodeDownException(msg: String)
  extends Exception(msg) with Serializable



object DbWriter {
  def props(databaseUrl: String) =
    Props(new DbWriter(databaseUrl))

  def name(databaseUrl: String) =
    s"""db-writer-${databaseUrl.split("/").last}"""

  case class Line(time: Long, message: String, messageType: String)
}


class DbWriter(databaseUrl: String) extends Actor {
  val connection = new DbCon(databaseUrl)

  import DbWriter._
  def receive = {
    case Line(time, message, messageType) =>
    //  write to DB connection
  }

  override def postStop(): Unit = {
    connection.close
  }
}


object LogProcessor {
  def props(databaseUrls: Vector[String]) =
    Props(new LogProcessor(databaseUrls))

  def name = s"log_processor_${UUID.randomUUID().toString}"

  case class LogFile(file: File)
}


class LogProcessor(databaseUrls: Vector[String])
  extends Actor with ActorLogging {
  require(databaseUrls.nonEmpty)

  val initialDatabaseUrl = databaseUrls.head
  var alternateDatabases = databaseUrls.tail

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
//    case _: DbBrokenConnectionException => Restart
    case _: DbNodeDownException => Stop
  }

  var dbWriter = context.actorOf(
    DbWriter.props(initialDatabaseUrl),
    DbWriter.name(initialDatabaseUrl)
  )

  context.watch(dbWriter)

  import LogProcessor._

  def receive = {
    case LogFile(file) =>
      //  file parsing and send to dbWriter

    case Terminated(_) =>
      if (alternateDatabases.nonEmpty) {
        val newDatabaseUrl = alternateDatabases.head
        alternateDatabases = alternateDatabases.tail
        dbWriter = context.actorOf(
          DbWriter.props(newDatabaseUrl),
          DbWriter.name(newDatabaseUrl)
        )
        context.watch(dbWriter)
      } else {
        log.error("All DB nodes broken, stopping.")
        self ! PoisonPill
      }
  }
}


class FileWatcher(source: String, databaseUrls: Vector[String])
  extends Actor with ActorLogging {
  override def supervisorStrategy = OneForOneStrategy() {
    case _: CorruptedFileException => Resume
  }

  val logProcessor = context.actorOf(
    LogProcessor.props(databaseUrls),
    LogProcessor.name
  )

  context.watch(logProcessor)

  def receive = {
    case Terminated(_) =>
      log.info(s"Log processor terminated, stopping file watcher.")
      self ! PoisonPill
  }
}



case class DbCon(databaseUrl: String) {
  def close: Unit = {}
}