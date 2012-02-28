package smr

import scala.collection.generic.CanBuildFrom

package object collection {

  class RichBuildFrom[From,Elem,To](cbf: CanBuildFrom[From,Elem,To]) {
    def ifDistributed[B](f: CanBuildDistributedFrom[From,Elem,To]=>B) = cbf match {
      case x: CanBuildDistributedFrom[From,Elem,To] => new Intermediate(Some(f(x)))
      case _ => new Intermediate(None)
    }

    class Intermediate[+B] private[RichBuildFrom] (b: Option[B]) {
      def otherwise[R>:B](r: =>R): R = b.getOrElse(r);
    }

  }

  protected[collection] implicit def richBuildFrom[From,Elem,To](cbf: CanBuildFrom[From,Elem,To]) = new RichBuildFrom(cbf);
}
