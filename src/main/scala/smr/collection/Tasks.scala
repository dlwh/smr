package smr.collection

import smr.Task
import collection.GenTraversableOnce

/**
 * 
 * @author dlwh
 */

private object Tasks {

}

class MapTask[T,U,LocalSummary,R](builder: LocalBuilder[U,R,LocalSummary], f: T=>U) extends Task[Iterable[T],R,LocalSummary] {
  def apply(iterable: Iterable[T]) = {
    val localBuilder = builder.copy // handle threading problems and such
    localBuilder.sizeHint(iterable);
    for(t <- iterable) {
      localBuilder += f(t)
    }

    val r = localBuilder.result();
    val sum = localBuilder.summary();
    (Some(r),sum);
  }
}

class FlatMapTask[T,U,LocalSummary,R](builder: LocalBuilder[U,R,LocalSummary], f: T=>GenTraversableOnce[U]) extends Task[Iterable[T],R,LocalSummary] {
  def apply(iterable: Iterable[T]) = {
//    builder.sizeHint(iterable);
    val localBuilder = builder.copy
    for(t <- iterable; u <- f(t)) {
      localBuilder += u
    }

    val r = localBuilder.result();
    val sum = localBuilder.summary();
    (Some(r),sum);
  }
}

class CollectTask[T,U,LocalSummary,R](builder: LocalBuilder[U,R,LocalSummary], f: PartialFunction[T,U]) extends Task[Iterable[T],R,LocalSummary] {
  def apply(iterable: Iterable[T]) = {
//    builder.sizeHint(iterable);
    val localBuilder = builder.copy
    for(t <- iterable if f.isDefinedAt(t)) {
      localBuilder += f(t)
    }

    val r = localBuilder.result();
    val sum = localBuilder.summary();
    (Some(r),sum);
  }
}

class FilterTask[T,LocalSummary,R](builder: LocalBuilder[T,R,LocalSummary], f: T=>Boolean) extends Task[Iterable[T],R,LocalSummary] {
  def apply(iterable: Iterable[T]) = {
    val localBuilder = builder.copy
    localBuilder.sizeHint(iterable);
    for(t <- iterable if f(t)) {
      localBuilder += t
    }

    val r = localBuilder.result();
    val sum = localBuilder.summary();
    (Some(r),sum);
  }
}

class RetrieveTask[T]() extends Task[Iterable[T],Int,Iterable[T]] {
  def apply(iterable:Iterable[T]) = (None,iterable);
}

class ForeachTask[T,U](f: T=> U) extends Task[Iterable[T],Int,Unit] {
  def apply(iterable: Iterable[T]) = {
    iterable foreach f;

    (None,())
  }
}

class ReduceTask[T,U>:T](op: (U,U)=>U) extends Task[Iterable[T],Int,U] {
  def apply(t: Iterable[T]) = (None,t.reduce(op));
}

class AggregateTask[T,U](z: U, seqOp: (U,T)=>U, op: (U,U)=>U) extends Task[Iterable[T],Int,U] {
  def apply(t: Iterable[T]) = (None,t.aggregate(z)(seqOp,op));
}
