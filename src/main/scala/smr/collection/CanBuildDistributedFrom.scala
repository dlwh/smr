package smr.collection

import collection.generic.CanBuildFrom

/**
 * 
 * @author dlwh
 */

trait CanBuildDistributedFrom[-This,B,That] extends CanBuildFrom[This,B,That] {
  def apply(t: This):DistributedBuilder[B,That]
  def apply():DistributedBuilder[B,That]
}
