package smr
package storage

/**
 * 
 * @author dlwh
 */
trait Storage {
  def store[T](name: String, t: T):Shard
  def store[T](t: T):Shard
  def load[T](uri: Shard):Option[T]
  def name(shards: IndexedSeq[Shard],name: String):IndexedSeq[Shard]
  def shardsFor(name: String):Option[IndexedSeq[Shard]]

  def forget(shards: IndexedSeq[Shard])
}

object Storage {
}
