package scala.collection

/**
 * 
 * @author dlwh
 */

trait HackGenTraversableLike[+A,+Repr] extends GenTraversableLike[A,Repr];
trait HackGenIterableLike[+A,+Repr] extends GenIterableLike[A,Repr] with HackGenTraversableLike[A,Repr];

