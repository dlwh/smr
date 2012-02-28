package smr

import java.io._
import java.util.zip.{GZIPInputStream, GZIPOutputStream}
import scala.collection.Parallelizable
import storage._
import actors.Futures
;

/**
 * 
 * @author dlwh
 */
abstract class ThreadDistributor extends Distributor with DistributorLike[ThreadDistributor] with Storage {
  protected def doWithShard[T, A](shard: Shard)(f: (Storage, T) => A) = {
    Futures.future(f(this,load[T](shard).get));
  }

  def doTasks[T,ToStore,ToReturn](shards: IndexedSeq[Shard], task: Task[T,ToStore,ToReturn]):IndexedSeq[(IndexedSeq[Shard],ToReturn)] = {
    {
      for(s <- shards.par) yield doWithShard[T,(IndexedSeq[Shard],ToReturn)](s) { (context,t:T) =>
        val (tostore, toreturn) = task(t);
        val uris = tostore.map(context.store(_)).toIndexedSeq;
        (uris,toreturn);
      }
    }.map(_.apply()).toIndexedSeq;
  }

}

object ThreadDistributor {

}
