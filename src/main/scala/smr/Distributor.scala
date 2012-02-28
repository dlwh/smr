package smr

import collection.{SimpleDistributedIterable, DistributedIterable}
import storage.Storage
import scala.collection.mutable.ArrayBuffer

/**
 * 
 * @author dlwh
 */

trait Distributor extends DistributorLike[Distributor] with Storage {
  def shutdown(): Unit = {}
  protected def defaultSizeHint = Runtime.getRuntime.availableProcessors()

  def doTasks[T,ToStore,ToReturn](shards: IndexedSeq[Shard], task: Task[T,ToStore,ToReturn]):IndexedSeq[(IndexedSeq[Shard],ToReturn)]
}

trait DistributorLike[+D<:Distributor] { this: D =>
  protected def repr:D = this
  def distribute[From,To](coll: From, shardHint: Int= -1)(implicit dist: CanDistribute[D,From,To]):To = {
    dist.distribute(this,coll,shardHint)
  }

  def loadCheckpoint[CC](name: String)(implicit checkpointLoader: CanLoadCheckpoint[D,CC]): Option[CC] = {
    checkpointLoader.load(this,name)
  }

  def checkpoint[CC,CCSaved](name: String)(block: =>CC)(implicit  checkpointSaver: CanSaveCheckpoint[D,CC,CCSaved],
                                                        checkpointLoader: CanLoadCheckpoint[D,CCSaved]):CCSaved = {
    checkpointLoader.load(repr.asInstanceOf[D],name).getOrElse(checkpointSaver.save(repr.asInstanceOf[D],name,block))
  }

  def saveCheckpoint[CC,CCSaved](name: String, cc: CC)(implicit checkpointSaver: CanSaveCheckpoint[D,CC,CCSaved]) = {
    checkpointSaver.save(this,name,cc)
  }

}

object Distributor {
  implicit def canDistributeIterables[T] = new CanDistribute[Distributor,Iterable[T],DistributedIterable[T]] {
    def distribute(d: Distributor, from: Iterable[T], hint: Int) = {
      val realhint = if(hint > 0) hint else d.defaultSizeHint
      from match {
        case from: IndexedSeq[T] if from.size < realhint =>
          val shard = d.store(from)
          new SimpleDistributedIterable[T](IndexedSeq(shard), IndexedSeq(from.size), d)
        case from: IndexedSeq[T] =>
          val offsets = (0 until from.size by ( (from.size/realhint) max 1)).take(realhint)
          val sizes = IndexedSeq.fill(offsets.size-1)(from.size / realhint) :+ (from.size % realhint + from.size / realhint)
          val pieces = offsets zip sizes map { case (offset,size) => d.store(from.slice(offset,offset + size):Iterable[T])}
          new SimpleDistributedIterable[T](pieces, sizes, d)
        case from: Iterable[T] =>
          val pieces = IndexedSeq.fill(realhint)(new ArrayBuffer[T]())
          for ( (t,i) <- from.zipWithIndex) pieces(i%realhint) += t
          new SimpleDistributedIterable[T](pieces.map(d.store(_:Iterable[T])), pieces.map(_.size), d)
      }
    }
  }
}
