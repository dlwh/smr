package smr.akka

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory

/**
 * 
 * @author dlwh
 */

@RunWith(classOf[JUnitRunner])
class AkkaDistributorTest extends FunSuite {

  val config = ConfigFactory.parseString("""
      akka {
        actor {
          serialize-messages = on
        }
      }
""")


  val system = ActorSystem("MySystem", config)
  val dist = new AkkaDistributor(system,4)

  test("Basics") {
    val x = 0 until 10000;
    assert(dist.distribute(x).map(_ * 2).sum === x.map(_ * 2).sum);
  }

  test("Basics with prime number of shards") {
    val x = 0 until 10000;
    assert(dist.distribute(x,3).map(_ * 2).sum === x.map(_ * 2).sum);
  }

}