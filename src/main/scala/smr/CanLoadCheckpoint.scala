package smr

/**
 * 
 * @author dlwh
 */

trait CanLoadCheckpoint[-D,+CC] {
  def load(d: D, name: String):Option[CC]
}

trait CanSaveCheckpoint[-D,-CC,+CCSaved] {
  def save(d: D, name: String, cc: CC):CCSaved
}
