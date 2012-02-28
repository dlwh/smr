package smr

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import storage.InMemoryStorage

/**
 * 
 * @author dlwh
 */

@RunWith(classOf[JUnitRunner])
class ThreadDistributorTest extends FunSuite {

  test("Basics") {
    val dist = new ThreadDistributor with InMemoryStorage;
    val x = 0 until 10000;
    assert(dist.distribute(x).map(_ * 2).sum === x.map(_ * 2).sum);
  }

  test("Basics with prime number of shards") {
    val dist = new ThreadDistributor with InMemoryStorage;
    val x = 0 until 10000;
    assert(dist.distribute(x,3).map(_ * 2).sum === x.map(_ * 2).sum);
  }


}