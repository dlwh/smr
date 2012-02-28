package smr

import java.net.URI

/**
 * 
 * @author dlwh
 */
case class Shard(uri: URI) {
  def this(s: String) = this(URI.create(s))
}
