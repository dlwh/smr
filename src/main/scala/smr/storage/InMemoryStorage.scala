package smr
package storage

import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable.{SynchronizedMap, ArrayBuffer, HashMap, WeakHashMap}
import java.net.URI

/**
 * 
 * @author dlwh
 */

trait InMemoryStorage extends Storage {
  def myPrefix: String = "mem"
  private val map = new HashMap[URI,Any] with SynchronizedMap[URI,Any]
  private val persistent = new HashMap[String,IndexedSeq[Any]] with SynchronizedMap[String,IndexedSeq[Any]]
  protected def default(uri: Shard):Option[Any] = sys.error("Not found: " + uri)
  private val anonctr = new AtomicLong(0)

  private def grabPersistent[T](shard: Shard): Option[T] = {
    val uri = shard.uri
    val path = uri.getPath
    val parts = path.split("/")
    persistent.get(parts(0)).map(_ apply parts(1).toInt).orElse(default(shard)).map(_.asInstanceOf[T])
  }

  def load[T](shard: Shard) =  {
    if(shard.uri.getScheme == "tmp")
      map.get(shard.uri).orElse(default(shard)).map(_.asInstanceOf[T])
    else {
      grabPersistent(shard)
    }
  }

  def store[T](t: T) = {
    val uri = new URI("tmp://"+myPrefix +"/"+anonctr.getAndIncrement)
    map(uri) = t
    new Shard(uri)
  }

  def store[T](name: String, t: T) = {
    val uri = new Shard("memory://"+ myPrefix+"/"+name+"/0")
    persistent(name) = IndexedSeq(t)
    uri
  }

  def forget(uri: Shard) { map -= uri.uri}

  def name(shards: IndexedSeq[Shard], name: String):IndexedSeq[Shard] = {
    val pieces = for( s <- shards) yield {
      if(s.uri.getScheme == "tmp") map(s.uri)
      else grabPersistent(s).get
    }
    persistent(name) = shards
    (0 until pieces.length) map (i => new Shard("memory://"+ myPrefix+"/"+name+"/"+i))
  }

  def shardsFor(name: String) = for {
    pieces <- persistent.get(name)
  } yield  (0 until pieces.length) map (i => new Shard("memory://"+ myPrefix+"/"+name+"/"+i))


  def forget(shards: IndexedSeq[Shard]) {
     map --= shards.map(_.uri)
  }
}

object InMemoryStorage {
  def apply(name: String):InMemoryStorage = {
    val n = name
    new InMemoryStorage with Serializable {
      override def myPrefix = n
    }
  }
}