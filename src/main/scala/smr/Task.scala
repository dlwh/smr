package smr

import java.net.URI

/**
 * 
 * @author dlwh
 */
trait Task[-T,ToStore,ToReturn] extends Serializable {
  def apply(t :T):(Iterable[ToStore],ToReturn)
}
