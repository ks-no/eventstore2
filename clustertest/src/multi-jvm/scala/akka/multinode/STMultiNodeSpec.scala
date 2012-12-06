package akka.multinode

import akka.remote.testkit.MultiNodeSpecCallbacks
import org.scalatest.matchers.MustMatchers
import org.scalatest.{BeforeAndAfterAll, WordSpec}

trait STMultiNodeSpec extends MultiNodeSpecCallbacks with WordSpec with MustMatchers with BeforeAndAfterAll {
  override def beforeAll() = multiNodeSpecBeforeAll()
  override def afterAll() = multiNodeSpecAfterAll()
}