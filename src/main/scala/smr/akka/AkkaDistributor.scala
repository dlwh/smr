package smr.akka

import smr.Shard
import smr.akka.Messages._
import smr.storage.{Storage, InMemoryStorage}
import akka.actor.{ActorRef, Props, Actor, ActorSystem}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.CountDownLatch
import akka.util.duration._
import akka.pattern.ask
import akka.util.{Timeout, Duration}
import akka.dispatch.{Await, Future}
import smr.{Task, Distributor}
import java.lang.ref.{WeakReference, ReferenceQueue}
import java.net.URI
import collection.mutable.{HashSet, HashMap, ArrayBuffer}
import com.typesafe.config.ConfigFactory

/**
 * 
 * @author dlwh
 */

class AkkaDistributor(system: ActorSystem, localWorkers: Int) extends Distributor {

  private def numWorkers = {
    implicit val timeout = Timeout(2.seconds)
    val future  = (master ? 'Workers)
    Await.result(future,timeout.duration).asInstanceOf[Int]
  }
  override protected def defaultSizeHint = numWorkers

  private val master = system.actorOf(Props(new AkkaDistributor.Master()), name="smrMaster")
  for(i <- 0 until localWorkers) {
    val storage = InMemoryStorage("local-" +i)
    val a = system.actorOf(Props(new AkkaWorker(storage)))
    registerWorker(a)
  }
  private val idGen = new AtomicLong(0)

  def registerWorker(a: ActorRef) = {
    master ! NewActor(a)
  }

  def storeTimeout = 10.minutes
  def loadTimeout = 10.minutes
  def executeTimeout = 10.minutes

  def store[T](name: String, t: T) = {
    implicit val timeout = Timeout(storeTimeout)
    val future = (master ? Store(idGen.getAndIncrement, t, Some(name))).mapTo[Shard]
    Await.result(future,timeout.duration)
  }

  def store[T](t: T) = {
    implicit val timeout = Timeout(storeTimeout)
    val future = (master ? Store(idGen.getAndIncrement, t, None)).mapTo[Shard]
    Await.result(future,timeout.duration)
  }

  def load[T](uri: Shard) = {
    implicit val timeout = Timeout(loadTimeout)
    val future = (master ? Retrieve(idGen.getAndIncrement, uri))
    Await.result(future,timeout.duration).asInstanceOf[Option[T]]
  }

  def name(shards: IndexedSeq[Shard], name: String):IndexedSeq[Shard] = {
    implicit val timeout = Timeout(loadTimeout)
    val future = (master ? Name(idGen.getAndIncrement, shards, name))
    Await.result(future,timeout.duration).asInstanceOf[IndexedSeq[Shard]]
  }

  def shardsFor(name: String) = {
    implicit val timeout = Timeout(loadTimeout)
    val future = (master ? QueryShards(idGen.getAndIncrement, name))
    Await.result(future,timeout.duration).asInstanceOf[Option[IndexedSeq[Shard]]]
  }

  def doTasks[T, ToStore, ToReturn](shards: IndexedSeq[Shard], task: Task[T, ToStore, ToReturn]) = {
    implicit val timeout = Timeout(executeTimeout)
    val future = (master ? DoTasks(idGen.getAndIncrement, shards, task.asInstanceOf[Task[Any,Any,Any]]))
    Await.result(future,timeout.duration).asInstanceOf[IndexedSeq[(IndexedSeq[Shard],ToReturn)]]
  }

  // managing memory on the dudes
  def forget(shards: IndexedSeq[Shard]) = {
    master ! Forget(shards)
  }
}

object AkkaDistributor {
  private class Master(initialWorkers: IndexedSeq[ActorRef] = IndexedSeq.empty) extends Actor {
    private[AkkaDistributor] val workers: ArrayBuffer[ActorRef] = new ArrayBuffer[ActorRef]() ++= initialWorkers


    private var nextWorker = 0;
    private val senders = new HashMap[Long,ActorRef]()
    private val shardLocations = new HashMap[URI,ActorRef]()
    private val shardRefs = new HashSet[ShardRef]()
    private val partialNames = new HashMap[Long, ArrayBuffer[Any]]()
    private val expectedResults = new HashMap[Long, Int]()

    def registerWorker(a: ActorRef) {
      self ! NewActor(a)
    }

    private def expunge() {

      val toForget = new ArrayBuffer[Shard]()
      var ok = true
      while(ok) {
        ok = false
        val ref = references.poll()
        if(ref != null) {
          toForget += Shard(ref.asInstanceOf[ShardRef].uri)
          ok = true
        }
      }

      self ! Forget(toForget)
    }

    private val references = new ReferenceQueue[Shard]()

    // hold string so we can remake and then purge
    private class ShardRef(shard: Shard) extends WeakReference(shard, references) {
      val uri = shard.uri
    }

    def trackShards(shards: scala.IndexedSeq[Shard]) {
      shards foreach { shard =>
        shardLocations += (shard.uri -> sender)
        shardRefs += new ShardRef(shard)
      }
    }

    protected def receive = {
      case m@Store(id, t, name) =>
        nextWorker %= workers.size
        senders(id) = sender
        workers(nextWorker) ! m
        nextWorker += 1
      case m@Stored(id, result) =>
        trackShards(IndexedSeq(result))
        senders(id) ! result
        senders -= id
      case m@Retrieve(id, shard) =>
        shardLocations.get(shard.uri) match {
          case Some(holder) =>
            senders(id) = sender
            holder ! m
          case None => sender ! Retrieved(id,None)
        }
      case m@Retrieved(id, t) =>
        senders(id) ! t
        senders -= id
      case m@Name(id, shards, name) =>
        expunge()
        senders(id) = sender
        val divvied = shards.groupBy( {(_:Shard).uri} andThen shardLocations)
        for( (loc,div)  <- divvied) {
          loc ! Name(id, div, name)
        }
        expectedResults(id) = divvied.size
      case n@Named(id, shards) =>
        expectedResults(id) -= 1
        val buf = partialNames.getOrElseUpdate(id,new ArrayBuffer[Any])
        buf ++= shards
        trackShards(shards)
        if(expectedResults(id) == 0) {
          expectedResults -= id
          partialNames -= id
          senders(id) ! buf
        }
      case m@QueryShards(id, name) =>
        senders(id) = sender
        workers foreach { _ ! m}
        expectedResults(id) = workers.size
      case n@ShardsFor(id, shards) =>
        expectedResults(id) -= 1
        val buf = partialNames.getOrElseUpdate(id,new ArrayBuffer[Any])
        buf ++= shards.getOrElse(IndexedSeq.empty)
        for(s <- shards) trackShards(s)
        if(expectedResults(id) == 0) {
          expectedResults -= id
          partialNames -= id
          senders(id) ! { if(buf.nonEmpty) Some(buf) else None}
        }
      case m@DoTasks(id,shards,task) =>
        expunge()
        senders(id) = sender
        val divvied = shards.groupBy( {(_:Shard).uri} andThen shardLocations)
        for( (loc,div)  <- divvied) {
          loc ! DoTasks(id, div, task)
        }
        expectedResults(id) = divvied.size
      case m@DoneTasks(id, results) =>
        expectedResults(id) -= 1
        val buf = partialNames.getOrElseUpdate(id,new ArrayBuffer[Any])
        buf ++= results
        trackShards(results.flatMap(_._1))
        if(expectedResults(id) == 0) {
          expectedResults -= id
          partialNames -= id
          senders(id) ! buf
        }
      case m@Forget(shards) =>
        val divvied = shards.groupBy( {(_:Shard).uri} andThen shardLocations)
        for( (loc,div)  <- divvied) {
          loc ! Forget(div)
        }
        for(s <- shards) {
          shardLocations -= s.uri

        }
      case NewActor(a) =>
        workers += a
      case 'Workers =>
        sender ! workers.length

    }
  }
}

class AkkaWorker(storage: Storage) extends Actor {
  protected def receive = {
    case Store(id, x,name) =>
      val result = name match {
        case None =>storage.store(x)
        case Some(n) => storage.store(n,x)
      }
      sender ! Stored(id,result)
    case Retrieve(id, uri) =>
      sender ! Retrieved(id, storage.load[Any](uri))
    case Name(id, shards, name) =>
      sender ! Named(id, storage.name(shards,name))
    case QueryShards(id: Long, name: String) =>
      sender ! ShardsFor(id, storage.shardsFor(name))
    case DoTasks(id, shards, task) =>
      val results = for ( shard <- shards; t = storage.load[Any](shard).get) yield {
        val (toStore: Iterable[Any],toReturn) = task(t)
        val stored = toStore.map(storage.store[Any](_))
        stored.toIndexedSeq -> toReturn
      }
      sender ! DoneTasks(id, results)
    case Forget(shards: IndexedSeq[Shard]) =>
      storage.forget(shards)

  }
}

object Messages {
  sealed trait ToWorkerMessage {
    def id: Long
  }
  case class Store(id: Long, x: Any, name: Option[String]) extends ToWorkerMessage
  case class Retrieve(id: Long, uri: Shard) extends ToWorkerMessage
  case class Forget(shards: IndexedSeq[Shard])
  case class Name(id: Long, uri: IndexedSeq[Shard], name: String) extends ToWorkerMessage
  case class QueryShards(id: Long, name: String) extends ToWorkerMessage
  case class DoTasks(id: Long, shards: IndexedSeq[Shard], task: Task[Any,Any,Any]) extends ToWorkerMessage

  sealed trait FromWorkerMessage {
    def id: Long
  }
  case class Stored(id: Long, uri: Shard) extends FromWorkerMessage
  case class Retrieved(id: Long, x: Any) extends FromWorkerMessage
  case class Named(id: Long, uri: IndexedSeq[Shard]) extends FromWorkerMessage
  case class ShardsFor(id: Long, uris: Option[IndexedSeq[Shard]]) extends FromWorkerMessage
  case class DoneTasks(id: Long, results: IndexedSeq[(IndexedSeq[Shard],Any)]) extends FromWorkerMessage


  case class NewActor(a: ActorRef)
}



object AkkaServer extends App {
  val config = ConfigFactory.parseString("""
akka {
  loglevel = DEBUG
  actor {
    provider = "akka.remote.RemoteActorRefProvider"
  }
  remote {
    transport = "akka.remote.netty.NettyRemoteTransport"
    netty {
      hostname = "127.0.0.1"
      port = 2552
    }
 }
}

""")
  val system = ActorSystem("server",config)
  val distributor = new AkkaDistributor(system, 0)

  Console.readLine()
  println(distributor.distribute(0 until 100).map(_ * 2).sum)
  println(distributor.distribute(0 until 100).map(_ * 2) foreach println)

}

object AkkaClient extends App {

  val config = ConfigFactory.parseString("""
akka {
  loglevel = DEBUG
  actor {
    provider = "akka.remote.RemoteActorRefProvider"
  }
  remote {
    transport = "akka.remote.netty.NettyRemoteTransport"
    netty {
      hostname = "127.0.0.1"
      port = 2553
    }
 }
}

""")

  val system = ActorSystem("client",config)
  val master = system.actorFor("akka://server@127.0.0.1:2552/user/smrMaster")
  val worker = system.actorOf(Props(new AkkaWorker(InMemoryStorage("remote-0"))))
  master ! NewActor(worker)

}