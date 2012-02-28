package smr
package collection

import storage._;

import util._;
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.{Buffer, ArrayBuffer, ArrayBuilder, Builder}
import scala.collection._

/**
 * 
 * @author dlwh
 */

trait DistributedIterable[+T] extends GenIterable[T] with DistributedIterableLike[T,DistributedIterable[T],Iterable[T]] with Sharded {


}

trait DistributedIterableLike[+T,+Repr<:DistributedIterable[T],+Local<:Iterable[T] with IterableLike[T,Local]] extends HackGenTraversableLike[T,Repr] {
  private def TODO = throw new RuntimeException("TODO")
  val distributor: Distributor with Storage;
  protected[this] val distributedBuilderFactory:CanBuildDistributedFrom[Repr,T,Repr];
  protected[this] def localBuilder:Builder[T,Local]
  def named(name: String):DistributedIterable[T]

  def isPersistent: Boolean


  def repr: Repr = this.asInstanceOf[Repr]
  def local: Local = {
    val b = localBuilder;
    for(t <- distributor.doTasks(repr.shards,new RetrieveTask[T]()).flatMap(_._2)) {
      b += t
    }
    b.result;
  }
  def seq = local;

  def collect[S, That](pf: PartialFunction[T, S])(implicit bf: CanBuildFrom[Repr, S, That]): That = bf ifDistributed { pbf =>
    val builder = pbf(repr)

    val results = distributor.doTasks(repr.shards, new CollectTask(builder.localBuilder,pf))
    builder.resultFromSummaries(results);
  } otherwise local.collect(pf)(bfToLocal(bf))


  def map[B,That](f: T=>B)(implicit cbf: CanBuildFrom[Repr,B,That]) = cbf ifDistributed { dbf =>
    val builder = dbf(repr);
    val results = distributor.doTasks(repr.shards,new MapTask(builder.localBuilder,f));

    builder.resultFromSummaries(results);
  } otherwise {
    local.map(f)(bfToLocal(cbf))
  }

  def aggregate[B](z: B)(seqop: (B, T) => B, combop: (B, B) => B) = {
    distributor.doTasks(repr.shards,new AggregateTask[T,B](z, seqop,combop)).map(_._2).reduce(combop);
  }
  def flatMap[B, That](f: (T) => GenTraversableOnce[B])(implicit bf: CanBuildFrom[Repr, B, That]) = bf ifDistributed { dbf =>
    val builder = dbf(repr);

    val results = distributor.doTasks(repr.shards,new FlatMapTask(builder.localBuilder,f));

    builder.resultFromSummaries(results);
  } otherwise {
    local.flatMap(f)(bfToLocal(bf))
  }

  def foreach[U](f: (T) => U) {
    distributor.doTasks(repr.shards, new ForeachTask(f));
  }

  def filter(pred: (T) => Boolean) = {
    val builder = distributedBuilderFactory apply (repr)
    val results =  distributor.doTasks(repr.shards,new FilterTask[T,builder.LocalSummary,Iterable[T]](builder.localBuilder, pred))
    builder.resultFromSummaries(results);
  }
  def filterNot(pred: (T)=>Boolean) = filter(pred andThen { ! _});

  def scan[B >: T, That](z: B)(op: (B, B) => B)(implicit cbf: CanBuildFrom[Repr, B, That]) = TODO

  def scanLeft[B, That](z: B)(op: (B, T) => B)(implicit cbf: CanBuildFrom[Repr, B, That]):That = local.scanLeft(z)(op)(bfToLocal(cbf));
  def scanRight[B, That](z: B)(op: (T, B) => B)(implicit cbf: CanBuildFrom[Repr, B, That]):That = local.scanRight(z)(op)(bfToLocal(cbf));


  def fold[A1 >: T](z: A1)(op: (A1, A1) => A1) = aggregate(z)(op,op);

  def reduce[U >: T](op: (U, U) => U) = {
    val results = distributor.doTasks(repr.shards, new ReduceTask[T,U](op));
    results.map(_._2).reduce(op);
  }
  def reduceOption[A1 >: T](op: (A1, A1) => A1) = if(isEmpty) None else Some(reduce(op));
  def forall(pred: (T) => Boolean) = TODO
  def exists(pred: (T) => Boolean) = TODO
  def find(pred: (T) => Boolean) = TODO

  def iterator = local.iterator;


  def reduceLeft[B >: T](op: (B, T) => B) = local.reduceLeft(op);
  def reduceRight[B >: T](op: (T, B) => B) = local.reduceRight(op);
  def reduceLeftOption[B >: T](op: (B, T) => B) = local.reduceLeftOption(op);
  def reduceRightOption[B >: T](op: (T, B) => B) = local.reduceRightOption(op);

  def min[A1 >: T](implicit ord: Ordering[A1]) = reduce((a,b) => if(ord.lt(a,b)) a else b);
  def max[A1 >: T](implicit ord: Ordering[A1]) = reduce((a,b) => if(ord.lt(a,b)) b else a);
  def maxBy[S](f: T => S)(implicit cmp: Ordering[S]): T = {
    if (repr.isEmpty) throw new UnsupportedOperationException("empty.maxBy")

    reduce((x, y) => if (cmp.gteq(f(x), f(y))) x else y)
  }

  def minBy[S](f: T => S)(implicit cmp: Ordering[S]): T = {
    if (repr.isEmpty) throw new UnsupportedOperationException("empty.minBy")

    reduce((x, y) => if (cmp.lteq(f(x), f(y))) x else y)
  }


  def sum[A1 >: T](implicit num: Numeric[A1]) = reduce(num.plus _)
  def product[A1 >: T](implicit num: Numeric[A1]) = reduce(num.times _)

  def /:[B](z: B)(op: (B, T) => B) = foldLeft(z)(op)
  def :\[B](z: B)(op: (T, B) => B) =  foldRight(z)(op)
  def foldLeft[B](z: B)(op: (B, T) => B) = local.foldLeft(z)(op);
  def foldRight[B](z:B)(op: (T,B)=>B) = local.foldRight(z)(op);


  def count(p: (T) => Boolean) = aggregate(0)({(x:Int,t: T) => if (p(t)) x+1 else x}, _ + _);

  def hasDefiniteSize: Boolean = true;
  def nonEmpty: Boolean = exists(_ => true);
//  def isEmpty = !nonEmpty;

  def size = aggregate(0)((x:Int, t: T)=>(x+1), _ + _);



  private[this] def bfToLocal[B,That](cbf: CanBuildFrom[Repr,B,That]) = new CanBuildFrom[Local,B,That] {
    def apply(from: Local) = cbf(repr);

    def apply() = cbf();
  }


  /* TODO: make these distributed collections */
  def toIndexedSeq[A1 >: T] = local.toIndexedSeq
  def toList = local.toList
  def toStream = local.toStream;
  def toIterator = local.toIterator
  def toSeq = local.toSeq
  def toSet[A1 >: T]:Set[A1] = local.toSet[A1]
  def toMap[K, V](implicit ev: <:<[T, (K, V)]) = local.toMap;
  def toBuffer[A1 >: T]:Buffer[A1] = local.toBuffer[A1];
  def toTraversable:GenTraversable[T] = repr;
  def toIterable = repr;

  def copyToArray[B >: T](xs: Array[B]) {local.copyToArray(xs)};
  def copyToArray[B >: T](xs: Array[B], start: Int) {local.copyToArray(xs,start)};
  def copyToArray[B >: T](xs: Array[B], start: Int, len: Int) {local.copyToArray(xs,start,len)};
  def toArray[A1 >: T](implicit evidence: ClassManifest[A1]) = local.toArray(evidence);

  def mkString:String = mkString("");
  def mkString(sep: String): String = mkString("",sep,"");
  def mkString(start: String, sep: String, end: String): String =  start + aggregate("")(_ + _.toString, _ + _) + end;
  override def toString = local.mkString(stringPrefix + "(", ", ", ")")
  def stringPrefix = "DistributedIterable";


  protected def exec[R,S,T](shard: Shard, task: Task[T,R,S]) = {
    distributor.doTasks(IndexedSeq(shard), task);
  }

  protected[this] def parCombiner = TODO

  def dropWhile(pred: (T) => Boolean) = TODO

  def span(pred: (T) => Boolean) = TODO

  def takeWhile(pred: (T) => Boolean) = TODO

  def splitAt(n: Int) = TODO

  def slice(unc_from: Int, unc_until: Int) = TODO

  def drop(n: Int) = TODO

  def take(n: Int) = TODO

  def groupBy[K](f: (T) => K) = TODO

  def partition(pred: (T) => Boolean) = TODO

  def ++[B >: T, That](that: GenTraversableOnce[B])(implicit bf: CanBuildFrom[DistributedIterable[T], B, That]) = TODO

  def zipAll[B, A1 >: T, That](that: GenIterable[B], thisElem: A1, thatElem: B)(implicit bf: CanBuildFrom[GenIterable[T], (A1, B), That]) = TODO

  def zipWithIndex[A1 >: T, That](implicit bf: CanBuildFrom[GenIterable[T], (A1, Int), That]) = TODO

  def zip[A1 >: T, B, That](that: GenIterable[B])(implicit bf: CanBuildFrom[GenIterable[T], (A1, B), That]) = TODO

  def sameElements[A1 >: T](that: GenIterable[A1]) = TODO

}

object DistributedIterable {
  implicit def builder[T,U]: CanBuildDistributedFrom[DistributedIterable[T],U,DistributedIterable[U]] = new CanBuildDistributedFrom[DistributedIterable[T],U,DistributedIterable[U]] {

    def apply(): DistributedBuilder[U, DistributedIterable[U]] = sys.error("TODO");

    def apply(t: DistributedIterable[T]): DistributedBuilder[U, DistributedIterable[U]]  = {
      new DistributedBuilder[U,DistributedIterable[U]]  {
        type LocalSummary = Int
        def summary() = innerBuilder.size;
        val innerBuilder = new ArrayBuffer[U];

        def +=(elem: U): this.type = {
          innerBuilder += elem;
          this;
        }

        def clear() {innerBuilder.clear()}
        def result() = sys.error("unsupported"); // ugh TODO
        def localBuilder = localSerBuilder[U];

        def resultFromSummaries(summaries: IndexedSeq[(Iterable[Shard], LocalSummary)]) = {
          val sizes = summaries.map(_._2);
          val uris = summaries.flatMap(_._1);
          new SimpleDistributedIterable[U](uris, sizes, t.distributor);
        }
      }
    }



  }

  private def localSerBuilder[Elem]:LocalBuilder[Elem,Iterable[Elem],Int] = new LocalBuilder[Elem,Iterable[Elem],Int] {
    def summary() = innerBuilder.size;
    val innerBuilder = new ArrayBuffer[Elem];
    def +=(elem: Elem): this.type = {
      innerBuilder += elem;
      this;
    }

    def result = innerBuilder;

    def copy = localSerBuilder[Elem]

    def clear() {innerBuilder.clear()};
  }
}

private[smr] class SimpleDistributedIterable[+T](val shards: IndexedSeq[Shard], sizes: IndexedSeq[Int],
                                                 val distributor: Distributor with Storage,
                                                 val isPersistent: Boolean = false) extends DistributedIterable[T] {
  protected[this] val distributedBuilderFactory:CanBuildDistributedFrom[DistributedIterable[T],T,DistributedIterable[T]] = DistributedIterable.builder[T,T]
  protected[this] def localBuilder = Iterable.canBuildFrom[T].apply();


  def named(name: String) = {
    val newShards = distributor.name(shards,name)
    new SimpleDistributedIterable[T](newShards,sizes,distributor, true)
  }

  override val size = sizes.foldLeft(0)(_ + _);

}
