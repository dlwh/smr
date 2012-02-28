package smr

/**
 *
 * @author dlwh
 */

trait CanDistribute[-D,-From,+To] {
  def distribute(d: D, from: From, hint: Int):To
}
