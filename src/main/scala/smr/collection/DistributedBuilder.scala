package smr.collection

import smr.Shard
import collection.mutable.Builder

/**
 * 
 * @author dlwh
 */

trait DistributedBuilder[Elem, +To] extends Builder[Elem,To] {
  type LocalSummary
  def localBuilder:LocalBuilder[Elem,Iterable[Elem],LocalSummary];
  def resultFromSummaries(summaries: IndexedSeq[(Iterable[Shard],LocalSummary)]):To;
}

trait LocalBuilder[-Elem, +To, LocalSummary] extends Builder[Elem, To] with Serializable {
  def summary():LocalSummary;
  def copy:LocalBuilder[Elem,To,LocalSummary]
}
