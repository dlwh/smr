package smr.collection

import smr.Shard

/**
 * 
 * @author dlwh
 */

trait Sharded {
  def shards: IndexedSeq[Shard];
}
