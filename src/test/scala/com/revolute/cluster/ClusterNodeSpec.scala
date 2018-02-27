package com.revolute.cluster

import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.revolute.cluster.model._
import com.revolute.cluster.model.system.{BlockStatuses, Blocks}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest._

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

// This test is not running during sbt cycle
// It's to have and not optimal
// Needed to be rewritten with akkaMultiNodeTestKit
@Ignore
class ClusterNodeSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {

  def config(group: Int, seed: Int, n: Int): Config =
    ConfigFactory.parseString(
      s"""
         akka.remote.netty.tcp.port=$group$n
         akka.remote.artery.canonical.port=$group$n
         akka.cluster.seed-nodes=["akka://ClusterSystem@127.0.0.1:$group$seed"]
      """).withFallback(ConfigFactory.load())

  implicit class CompareHandler[T](future: Future[T]) {
    def `should equal`[V](value: V): Assertion =
      Await.result(future, 5 second) shouldEqual value
  }

  "A three-node cluster" must {

    "rebalance blocks after second and third node were connected and disconnected" in {
      val group = 255
      val seed  = 1

      val system1 = ActorSystem("ClusterSystem", config(group, seed, 1))

      val node1 = system1.actorOf(Props(new ClusterNode[String](10)), name = "node")

      val emptyBlock = Block(mutable.HashMap[Long, mutable.ListBuffer[Transaction]]())

      Thread.sleep(20000)

      implicit val timeout: Timeout = Timeout(5 seconds)

      (node1 ? system.GetNodeStatus) `should equal` Running

      (node1 ? system.GetBlockTypes) `should equal`
        system.BlockTypes(ArrayBuffer((8, Original), (2, Original), (5, Original), (4, Original), (7, Original), (1, Original), (9, Original), (3, Original), (6, Original), (0, Original)))

      (node1 ? system.GetBlockStatuses) `should equal`
        system.BlockStatuses(ArrayBuffer((8, Active), (2, Active), (5, Active), (4, Active), (7, Active), (1, Active), (9, Active), (3, Active), (6, Active), (0, Active)))

      (node1 ? system.GetBlocks) `should equal`
        system.Blocks(ArrayBuffer((8, emptyBlock), (2, emptyBlock), (5, emptyBlock), (4, emptyBlock), (7, emptyBlock), (1, emptyBlock), (9, emptyBlock), (3, emptyBlock), (6, emptyBlock), (0, emptyBlock)))


      // Add node2 to the cluster
      val system2 = ActorSystem("ClusterSystem", config(group, seed, 2))

      val node2 = system2.actorOf(Props(new ClusterNode[String](10)), name = "node")

      Thread.sleep(20000)

      (node1 ? system.GetNodeStatus) `should equal` Running
      (node2 ? system.GetNodeStatus) `should equal` Running

      // Check rebalancing
      (node1 ? system.GetBlockTypes) `should equal`
        system.BlockTypes(ArrayBuffer((8, Recovery), (2, Original), (5, Recovery), (4, Original), (7, Recovery), (1, Original), (9, Recovery), (3, Original), (6, Recovery), (0, Original)))

      (node2 ? system.GetBlockTypes) `should equal`
        system.BlockTypes(ArrayBuffer((8, Original), (2, Recovery), (5, Original), (4, Recovery), (7, Original), (1, Recovery), (9, Original), (3, Recovery), (6, Original), (0, Recovery)))


      (node1 ? system.GetBlockStatuses) `should equal`
        system.BlockStatuses(ArrayBuffer((8, Active), (2, Active), (5, Active), (4, Active), (7, Active), (1, Active), (9, Active), (3, Active), (6, Active), (0, Active)))

      (node2 ? system.GetBlockStatuses) `should equal`
        system.BlockStatuses(ArrayBuffer((8, Active), (2, Active), (5, Active), (4, Active), (7, Active), (1, Active), (9, Active), (3, Active), (6, Active), (0, Active)))


      (node1 ? system.GetBlocks) `should equal`
        system.Blocks(ArrayBuffer((8, emptyBlock), (2, emptyBlock), (5, emptyBlock), (4, emptyBlock), (7, emptyBlock), (1, emptyBlock), (9, emptyBlock), (3, emptyBlock), (6, emptyBlock), (0, emptyBlock)))

      (node2 ? system.GetBlocks) `should equal`
        system.Blocks(ArrayBuffer((8, emptyBlock), (2, emptyBlock), (5, emptyBlock), (4, emptyBlock), (7, emptyBlock), (1, emptyBlock), (9, emptyBlock), (3, emptyBlock), (6, emptyBlock), (0, emptyBlock)))


      // Add node3 to the cluster
      val system3 = ActorSystem("ClusterSystem", config(group, seed, 3))

      val node3 = system3.actorOf(Props(new ClusterNode[String](10)), name = "node")

      Thread.sleep(20000)

      (node1 ? system.GetNodeStatus) `should equal` Running
      (node2 ? system.GetNodeStatus) `should equal` Running
      (node3 ? system.GetNodeStatus) `should equal` Running


      // Check rebalancing
      (node1 ? system.GetBlockTypes) `should equal`
        system.BlockTypes(ArrayBuffer((2, Original), (5, Recovery), (4, Recovery), (1, Original), (3, Recovery), (0, Original)))

      (node2 ? system.GetBlockTypes) `should equal`
        system.BlockTypes(ArrayBuffer((8, Recovery), (5, Original), (4, Original), (7, Recovery), (9, Recovery), (3, Original), (6, Recovery)))

      (node3 ? system.GetBlockTypes) `should equal`
        system.BlockTypes(ArrayBuffer((8, Original), (2, Recovery), (7, Original), (1, Recovery), (9, Original), (6, Original), (0, Recovery)))


      (node1 ? system.GetBlockStatuses) `should equal`
        system.BlockStatuses(ArrayBuffer((2, Active), (5, Active), (4, Active), (1, Active), (3, Active), (0, Active)))

      (node2 ? system.GetBlockStatuses) `should equal`
        system.BlockStatuses(ArrayBuffer((8, Active), (5, Active), (4, Active), (7, Active), (9, Active), (3, Active), (6, Active)))

      (node3 ? system.GetBlockStatuses) `should equal`
        system.BlockStatuses(ArrayBuffer((8, Active), (2, Active), (7, Active), (1, Active), (9, Active), (6, Active), (0, Active)))


      (node1 ? system.GetBlocks) `should equal`
        system.Blocks(ArrayBuffer((2, emptyBlock), (5, emptyBlock), (4, emptyBlock), (1, emptyBlock), (3, emptyBlock), (0, emptyBlock)))

      (node2 ? system.GetBlocks) `should equal`
        system.Blocks(ArrayBuffer((8, emptyBlock), (5, emptyBlock), (4, emptyBlock), (7, emptyBlock), (9, emptyBlock), (3, emptyBlock), (6, emptyBlock)))

      (node3 ? system.GetBlocks) `should equal`
        system.Blocks(ArrayBuffer((8, emptyBlock), (2, emptyBlock), (7, emptyBlock), (1, emptyBlock), (9, emptyBlock), (6, emptyBlock), (0, emptyBlock)))


      // Remove node3 from the cluster
      system3.terminate()
      // It dies very slowly
      Thread.sleep(30000)

      // Check rebalancing
      (node1 ? system.GetBlockTypes) `should equal`
        system.BlockTypes(ArrayBuffer((8, Recovery), (2, Original), (5, Recovery), (4, Original), (7, Recovery), (1, Original), (9, Recovery), (3, Original), (6, Recovery), (0, Original)))

      (node2 ? system.GetBlockTypes) `should equal`
        system.BlockTypes(ArrayBuffer((8, Original), (2, Recovery), (5, Original), (4, Recovery), (7, Original), (1, Recovery), (9, Original), (3, Recovery), (6, Original), (0, Recovery)))


      (node1 ? system.GetBlockStatuses) `should equal`
        system.BlockStatuses(ArrayBuffer((8, Active), (2, Active), (5, Active), (4, Active), (7, Active), (1, Active), (9, Active), (3, Active), (6, Active), (0, Active)))

      (node2 ? system.GetBlockStatuses) `should equal`
        system.BlockStatuses(ArrayBuffer((8, Active), (2, Active), (5, Active), (4, Active), (7, Active), (1, Active), (9, Active), (3, Active), (6, Active), (0, Active)))


      (node1 ? system.GetBlocks) `should equal`
        system.Blocks(ArrayBuffer((8, emptyBlock), (2, emptyBlock), (5, emptyBlock), (4, emptyBlock), (7, emptyBlock), (1, emptyBlock), (9, emptyBlock), (3, emptyBlock), (6, emptyBlock), (0, emptyBlock)))

      (node2 ? system.GetBlocks) `should equal`
        system.Blocks(ArrayBuffer((8, emptyBlock), (2, emptyBlock), (5, emptyBlock), (4, emptyBlock), (7, emptyBlock), (1, emptyBlock), (9, emptyBlock), (3, emptyBlock), (6, emptyBlock), (0, emptyBlock)))


      // Remove nod 2
      system2.terminate()
      // It dies very slowly
      Thread.sleep(30000)

      // Check rebalancing
      (node1 ? system.GetBlockTypes) `should equal`
        system.BlockTypes(ArrayBuffer((8, Original), (2, Original), (5, Original), (4, Original), (7, Original), (1, Original), (9, Original), (3, Original), (6, Original), (0, Original)))

      (node1 ? system.GetBlockStatuses) `should equal`
        system.BlockStatuses(ArrayBuffer((8, Active), (2, Active), (5, Active), (4, Active), (7, Active), (1, Active), (9, Active), (3, Active), (6, Active), (0, Active)))

      (node1 ? system.GetBlocks) `should equal`
        system.Blocks(ArrayBuffer((8, emptyBlock), (2, emptyBlock), (5, emptyBlock), (4, emptyBlock), (7, emptyBlock), (1, emptyBlock), (9, emptyBlock), (3, emptyBlock), (6, emptyBlock), (0, emptyBlock)))


      system1.terminate()

      Thread.sleep(60000)
    }

  }

  "A one-node cluster" must {

    "lock appropriate blocks in an appropriate status" in {

      val group = 265
      val seed  = 1

      val system1 = ActorSystem("ClusterSystem", config(group, seed, 1))
      val partitions = 10

      val node1 = system1.actorOf(Props(new ClusterNode[String](partitions)), name = "node")

      Thread.sleep(20000)

      implicit val timeout: Timeout = Timeout(5 seconds)

      // Lock all blocks with transaction id = 1
      for(i <- 0 until partitions ) {
        (node1 ? system.WriteLock(index = i % partitions, transactionId = 1)) `should equal`
          system.WriteLocked(index = i % partitions, transactionId = 1)
      }

      // Lock the same blocks with the same transaction id = 1
      for(i <- 0 until partitions ) {
        (node1 ? system.WriteLock(index = i % partitions, transactionId = 1)) `should equal`
          system.WriteLocked(index = i % partitions, transactionId = 1)
      }

      // Lock the same blocks with another transaction id = 2
      for(i <- 0 until partitions ) {
        (node1 ? system.WriteLock(index = i % partitions, transactionId = 2)) `should equal`
          system.ExternallyLocked(index = i % partitions, transactionId = 2)
      }

      // Lock non-existing index
      for(i <- partitions until 2 * partitions ) {
        (node1 ? system.WriteLock(index = i, transactionId = 1)) `should equal`
          system.BrokenIndex(index = i, transactionId = 1)
      }

      (node1 ? system.GetBlockStatuses) `should equal`
        BlockStatuses(ArrayBuffer(
          (8, WriteLock(1)),
          (2, WriteLock(1)),
          (5, WriteLock(1)),
          (4, WriteLock(1)),
          (7, WriteLock(1)),
          (1, WriteLock(1)),
          (9, WriteLock(1)),
          (3, WriteLock(1)),
          (6, WriteLock(1)),
          (0, WriteLock(1))
        ))

      system1.terminate()

      Thread.sleep(60000)
    }
  }

  "A one-node cluster" must {

    "unlock appropriate blocks in an appropriate status" in {

      val group = 275
      val seed  = 1

      val system1 = ActorSystem("ClusterSystem", config(group, seed, 1))
      val partitions = 10

      val node1 = system1.actorOf(Props(new ClusterNode[String](partitions)), name = "node")

      Thread.sleep(20000)

      implicit val timeout: Timeout = Timeout(5 seconds)

      // Unlock not locked blocks with transaction id = 1
      for(i <- 0 until partitions ) {
        (node1 ? system.WriteUnlock(index = i % partitions, transactionId = 1)) `should equal`
          system.WriteUnlocked(index = i % partitions, transactionId = 1)
      }

      (node1 ? system.GetBlockStatuses) `should equal`
        BlockStatuses(ArrayBuffer(
          (8, Active),
          (2, Active),
          (5, Active),
          (4, Active),
          (7, Active),
          (1, Active),
          (9, Active),
          (3, Active),
          (6, Active),
          (0, Active)
        ))

      // Lock blocks with transaction id = 1
      for(i <- 0 until partitions ) {
        (node1 ? system.WriteLock(index = i % partitions, transactionId = 1)) `should equal`
          system.WriteLocked(index = i % partitions, transactionId = 1)
      }

      // Unlock locked blocks with transaction id = 2
      for(i <- 0 until partitions ) {
        (node1 ? system.WriteUnlock(index = i % partitions, transactionId = 2)) `should equal`
          system.ExternallyLocked(index = i % partitions, transactionId = 2)
      }

      (node1 ? system.GetBlockStatuses) `should equal`
        BlockStatuses(ArrayBuffer(
          (8, WriteLock(1)),
          (2, WriteLock(1)),
          (5, WriteLock(1)),
          (4, WriteLock(1)),
          (7, WriteLock(1)),
          (1, WriteLock(1)),
          (9, WriteLock(1)),
          (3, WriteLock(1)),
          (6, WriteLock(1)),
          (0, WriteLock(1))
        ))

      // Unlock locked blocks with transaction id = 1
      for(i <- 0 until partitions ) {
        (node1 ? system.WriteUnlock(index = i % partitions, transactionId = 1)) `should equal`
          system.WriteUnlocked(index = i % partitions, transactionId = 1)
      }

      (node1 ? system.GetBlockStatuses) `should equal`
        BlockStatuses(ArrayBuffer(
          (8, Active),
          (2, Active),
          (5, Active),
          (4, Active),
          (7, Active),
          (1, Active),
          (9, Active),
          (3, Active),
          (6, Active),
          (0, Active)
        ))

      // Unlock non-existing index
      for(i <- partitions until 2 * partitions ) {
        (node1 ? system.WriteUnlock(index = i, transactionId = 1)) `should equal`
          system.BrokenIndex(index = i, transactionId = 1)
      }

      BlockStatuses(ArrayBuffer(
        (8, Active),
        (2, Active),
        (5, Active),
        (4, Active),
        (7, Active),
        (1, Active),
        (9, Active),
        (3, Active),
        (6, Active),
        (0, Active)
      ))

      system1.terminate()

      Thread.sleep(60000)
    }
  }

  "A one-node cluster" must {

    "append records into the appropriate block" in {

      val group = 285
      val seed  = 1

      val system1 = ActorSystem("ClusterSystem", config(group, seed, 1))
      val partitions = 10

      val node1 = system1.actorOf(Props(new ClusterNode[String](partitions)), name = "node")

      Thread.sleep(20000)

      implicit val timeout: Timeout = Timeout(5 seconds)


      // Append records in not locked not recovery block
      for(i <- 0 until partitions ) {
        (node1 ? system.Append(index = i % partitions, transactionId = 1, key = i, record = s"record #$i-0")) `should equal`
          system.NonLockedAppend(index = i % partitions, transactionId = 1, key = i)
      }

      // Lock blocks with transaction id = 1 and append record
      for(i <- 0 until partitions ) {
        (node1 ? system.WriteLock(index = i % partitions, transactionId = 1)) `should equal`
          system.WriteLocked(index = i % partitions, transactionId = 1)

        (node1 ? system.Append(index = i % partitions, transactionId = 1, key = i, record = s"record #$i-0")) `should equal`
          system.Appended(index = i % partitions, transactionId = 1, key = i)
      }

      (node1 ? system.GetBlocks) `should equal`
        Blocks(ArrayBuffer(
          (8, Block(mutable.HashMap(8L -> ListBuffer("record #8-0")))),
          (2, Block(mutable.HashMap(2L -> ListBuffer("record #2-0")))),
          (5, Block(mutable.HashMap(5L -> ListBuffer("record #5-0")))),
          (4, Block(mutable.HashMap(4L -> ListBuffer("record #4-0")))),
          (7, Block(mutable.HashMap(7L -> ListBuffer("record #7-0")))),
          (1, Block(mutable.HashMap(1L -> ListBuffer("record #1-0")))),
          (9, Block(mutable.HashMap(9L -> ListBuffer("record #9-0")))),
          (3, Block(mutable.HashMap(3L -> ListBuffer("record #3-0")))),
          (6, Block(mutable.HashMap(6L -> ListBuffer("record #6-0")))),
          (0, Block(mutable.HashMap(0L -> ListBuffer("record #0-0"))))
      ))

      for(i <- 0 until 2 * partitions ) {
        // Add two records for each key
        (node1 ? system.Append(index = i % partitions, transactionId = 1, key = i, record = s"record #$i-1")) `should equal`
          system.Appended(index = i % partitions, transactionId = 1, key = i)

        (node1 ? system.Append(index = i % partitions, transactionId = 1, key = i, record = s"record #$i-2")) `should equal`
          system.Appended(index = i % partitions, transactionId = 1, key = i)
      }

      println(Await.result(node1 ? system.GetBlocks, 1 second))

      (node1 ? system.GetBlocks) `should equal`
        Blocks(ArrayBuffer(
          (8, Block(mutable.HashMap(8L -> ListBuffer("record #8-0", "record #8-1", "record #8-2"), 18L -> ListBuffer("record #18-1", "record #18-2")))),
          (2, Block(mutable.HashMap(2L -> ListBuffer("record #2-0", "record #2-1", "record #2-2"), 12L -> ListBuffer("record #12-1", "record #12-2")))),
          (5, Block(mutable.HashMap(5L -> ListBuffer("record #5-0", "record #5-1", "record #5-2"), 15L -> ListBuffer("record #15-1", "record #15-2")))),
          (4, Block(mutable.HashMap(14L -> ListBuffer("record #14-1", "record #14-2"), 4L -> ListBuffer("record #4-0", "record #4-1", "record #4-2")))),
          (7, Block(mutable.HashMap(17L -> ListBuffer("record #17-1", "record #17-2"), 7L -> ListBuffer("record #7-0", "record #7-1", "record #7-2")))),
          (1, Block(mutable.HashMap(11L -> ListBuffer("record #11-1", "record #11-2"), 1L -> ListBuffer("record #1-0", "record #1-1", "record #1-2")))),
          (9, Block(mutable.HashMap(19L -> ListBuffer("record #19-1", "record #19-2"), 9L -> ListBuffer("record #9-0", "record #9-1", "record #9-2")))),
          (3, Block(mutable.HashMap(13L -> ListBuffer("record #13-1", "record #13-2"), 3L -> ListBuffer("record #3-0", "record #3-1", "record #3-2")))),
          (6, Block(mutable.HashMap(16L -> ListBuffer("record #16-1", "record #16-2"), 6L -> ListBuffer("record #6-0", "record #6-1", "record #6-2")))),
          (0, Block(mutable.HashMap(10L -> ListBuffer("record #10-1", "record #10-2"), 0L -> ListBuffer("record #0-0", "record #0-1", "record #0-2"))))
        ))

      system1.terminate()

      Thread.sleep(60000)
    }
  }

  "A one-node cluster" must {

    "read records from the appropriate block" in {

      val group = 325
      val seed  = 1

      val system1 = ActorSystem("ClusterSystem", config(group, seed, 1))
      val partitions = 10

      val node1 = system1.actorOf(Props(new ClusterNode[String](partitions)), name = "node")

      Thread.sleep(20000)

      implicit val timeout: Timeout = Timeout(5 seconds)

      val transactionId = 1
      val fakeTransactionId = 2


      for(i <- 0 until partitions ) {
        // Read records from the wrong index or key
        (node1 ? system.Read(index = i % partitions, transactionId, key = i)) `should equal`
          system.NonLockedRead(index = i % partitions, transactionId, key = i)

        // Lock blocks with transaction id = 1 and read records
        (node1 ? system.WriteLock(index = i % partitions, transactionId)) `should equal`
          system.WriteLocked(index = i % partitions, transactionId)

        (node1 ? system.Append(index = i % partitions, transactionId, key = i, record = s"record #$i-0")) `should equal`
          system.Appended(index = i % partitions, transactionId, key = i)

        (node1 ? system.Read(index = i % partitions, transactionId, key = i)) `should equal`
          system.Records(index = i % partitions, transactionId, key = i, records = Seq[String](s"record #$i-0"))

        // Read records with fake transaction id
        (node1 ? system.Read(index = i % partitions, fakeTransactionId, key = i)) `should equal`
          system.ExternallyLocked(index = i % partitions, fakeTransactionId)

      }

      system1.terminate()

      Thread.sleep(60000)
    }
  }



  "A three-node cluster" must {

    "lock appropriate blocks in an appropriate status" in {

      val group = 295
      val seed  = 1

      val system1 = ActorSystem("ClusterSystem", config(group, seed, 1))
      val partitions = 10
      val emptyBlock = Block(mutable.HashMap[Long, mutable.ListBuffer[String]]())
      val transactionId = 1
      implicit val timeout: Timeout = Timeout(5 seconds)

      val node1 = system1.actorOf(Props(new ClusterNode[String](partitions)), name = "node")

      Thread.sleep(20000)

      // Add node2 to the cluster
      val system2 = ActorSystem("ClusterSystem", config(group, seed, 2))

      val node2 = system2.actorOf(Props(new ClusterNode[String](partitions)), name = "node")

      Thread.sleep(20000)

      // Add node3 to the cluster
      val system3 = ActorSystem("ClusterSystem", config(group, seed, 3))

      val node3 = system3.actorOf(Props(new ClusterNode[String](partitions)), name = "node")

      Thread.sleep(20000)

      // Check rebalancing. Yet another time. I don't believe in this magic
      (node1 ? system.GetBlockTypes) `should equal`
        system.BlockTypes(ArrayBuffer((2, Original), (5, Recovery), (4, Recovery), (1, Original), (3, Recovery), (0, Original)))

      (node2 ? system.GetBlockTypes) `should equal`
        system.BlockTypes(ArrayBuffer((8, Recovery), (5, Original), (4, Original), (7, Recovery), (9, Recovery), (3, Original), (6, Recovery)))

      (node3 ? system.GetBlockTypes) `should equal`
        system.BlockTypes(ArrayBuffer((8, Original), (2, Recovery), (7, Original), (1, Recovery), (9, Original), (6, Original), (0, Recovery)))


      (node1 ? system.GetBlockStatuses) `should equal`
        system.BlockStatuses(ArrayBuffer((2, Active), (5, Active), (4, Active), (1, Active), (3, Active), (0, Active)))

      (node2 ? system.GetBlockStatuses) `should equal`
        system.BlockStatuses(ArrayBuffer((8, Active), (5, Active), (4, Active), (7, Active), (9, Active), (3, Active), (6, Active)))

      (node3 ? system.GetBlockStatuses) `should equal`
        system.BlockStatuses(ArrayBuffer((8, Active), (2, Active), (7, Active), (1, Active), (9, Active), (6, Active), (0, Active)))


      (node1 ? system.GetBlocks) `should equal`
        system.Blocks(ArrayBuffer((2, emptyBlock), (5, emptyBlock), (4, emptyBlock), (1, emptyBlock), (3, emptyBlock), (0, emptyBlock)))

      (node2 ? system.GetBlocks) `should equal`
        system.Blocks(ArrayBuffer((8, emptyBlock), (5, emptyBlock), (4, emptyBlock), (7, emptyBlock), (9, emptyBlock), (3, emptyBlock), (6, emptyBlock)))

      (node3 ? system.GetBlocks) `should equal`
        system.Blocks(ArrayBuffer((8, emptyBlock), (2, emptyBlock), (7, emptyBlock), (1, emptyBlock), (9, emptyBlock), (6, emptyBlock), (0, emptyBlock)))

      // Client able to send message to any node
      // Node will rote the message to the appropriate node

      // Lock key located in node1 by request to node1
      (node1 ? client.Lock(transactionId, 2)) `should equal` client.Locked(transactionId, 2)

      // Lock key located in node2 by request to node1
      (node1 ? client.Lock(transactionId, 5)) `should equal` client.Locked(transactionId, 5)

      // Lock key located in node3 by request to node1
      (node1 ? client.Lock(transactionId, 8)) `should equal` client.Locked(transactionId, 8)

      // Check blocks
      // Only origin blocks should be locked
      (node1 ? system.GetBlockStatuses) `should equal`
        system.BlockStatuses(ArrayBuffer((2, WriteLock(transactionId)/*First lock*/), (5, Active), (4, Active), (1, Active), (3, Active), (0, Active)))

      (node2 ? system.GetBlockStatuses) `should equal`
        system.BlockStatuses(ArrayBuffer((8, Active), (5, WriteLock(transactionId)/*Second lock*/), (4, Active), (7, Active), (9, Active), (3, Active), (6, Active)))

      (node3 ? system.GetBlockStatuses) `should equal`
        system.BlockStatuses(ArrayBuffer((8, WriteLock(transactionId)/*Third lock*/), (2, Active), (7, Active), (1, Active), (9, Active), (6, Active), (0, Active)))


      system3.terminate()
      system2.terminate()
      system1.terminate()

      Thread.sleep(60000)

    }
  }


  "A three-node cluster" must {

    "unlock appropriate blocks in an appropriate status" in {

      val group = 305
      val seed  = 1

      val system1 = ActorSystem("ClusterSystem", config(group, seed, 1))
      val partitions = 10
      val emptyBlock = Block(mutable.HashMap[Long, mutable.ListBuffer[String]]())
      val transactionId = 1
      implicit val timeout: Timeout = Timeout(5 seconds)

      val node1 = system1.actorOf(Props(new ClusterNode[String](partitions)), name = "node")

      Thread.sleep(20000)

      // Add node2 to the cluster
      val system2 = ActorSystem("ClusterSystem", config(group, seed, 2))

      val node2 = system2.actorOf(Props(new ClusterNode[String](partitions)), name = "node")

      Thread.sleep(20000)

      // Add node3 to the cluster
      val system3 = ActorSystem("ClusterSystem", config(group, seed, 3))

      val node3 = system3.actorOf(Props(new ClusterNode[String](partitions)), name = "node")

      Thread.sleep(20000)

      // Check rebalancing. Yet another time. I don't believe in this magic
      (node1 ? system.GetBlockTypes) `should equal`
        system.BlockTypes(ArrayBuffer((2, Original), (5, Recovery), (4, Recovery), (1, Original), (3, Recovery), (0, Original)))

      (node2 ? system.GetBlockTypes) `should equal`
        system.BlockTypes(ArrayBuffer((8, Recovery), (5, Original), (4, Original), (7, Recovery), (9, Recovery), (3, Original), (6, Recovery)))

      (node3 ? system.GetBlockTypes) `should equal`
        system.BlockTypes(ArrayBuffer((8, Original), (2, Recovery), (7, Original), (1, Recovery), (9, Original), (6, Original), (0, Recovery)))


      (node1 ? system.GetBlockStatuses) `should equal`
        system.BlockStatuses(ArrayBuffer((2, Active), (5, Active), (4, Active), (1, Active), (3, Active), (0, Active)))

      (node2 ? system.GetBlockStatuses) `should equal`
        system.BlockStatuses(ArrayBuffer((8, Active), (5, Active), (4, Active), (7, Active), (9, Active), (3, Active), (6, Active)))

      (node3 ? system.GetBlockStatuses) `should equal`
        system.BlockStatuses(ArrayBuffer((8, Active), (2, Active), (7, Active), (1, Active), (9, Active), (6, Active), (0, Active)))


      (node1 ? system.GetBlocks) `should equal`
        system.Blocks(ArrayBuffer((2, emptyBlock), (5, emptyBlock), (4, emptyBlock), (1, emptyBlock), (3, emptyBlock), (0, emptyBlock)))

      (node2 ? system.GetBlocks) `should equal`
        system.Blocks(ArrayBuffer((8, emptyBlock), (5, emptyBlock), (4, emptyBlock), (7, emptyBlock), (9, emptyBlock), (3, emptyBlock), (6, emptyBlock)))

      (node3 ? system.GetBlocks) `should equal`
        system.Blocks(ArrayBuffer((8, emptyBlock), (2, emptyBlock), (7, emptyBlock), (1, emptyBlock), (9, emptyBlock), (6, emptyBlock), (0, emptyBlock)))

      // Client able to send message to any node
      // Node will rote the message to the appropriate node

      // Unlock not locked key located in node1 by request to node1
      (node1 ? client.Unlock(transactionId, 2)) `should equal` client.Unlocked(transactionId, 2)

      //  Unlock not locked key located in node2 by request to node1
      (node1 ? client.Unlock(transactionId, 5)) `should equal` client.Unlocked(transactionId, 5)

      //  Unlock not locked key located in node3 by request to node1
      (node1 ? client.Unlock(transactionId, 8)) `should equal` client.Unlocked(transactionId, 8)

      (node1 ? system.GetBlockStatuses) `should equal`
        system.BlockStatuses(ArrayBuffer((2, Active), (5, Active), (4, Active), (1, Active), (3, Active), (0, Active)))

      (node2 ? system.GetBlockStatuses) `should equal`
        system.BlockStatuses(ArrayBuffer((8, Active), (5, Active), (4, Active), (7, Active), (9, Active), (3, Active), (6, Active)))

      (node3 ? system.GetBlockStatuses) `should equal`
        system.BlockStatuses(ArrayBuffer((8, Active), (2, Active), (7, Active), (1, Active), (9, Active), (6, Active), (0, Active)))



      // Lock key located in node1 by request to node1
      (node1 ? client.Lock(transactionId, 2)) `should equal` client.Locked(transactionId, 2)

      // Lock key located in node2 by request to node1
      (node1 ? client.Lock(transactionId, 5)) `should equal` client.Locked(transactionId, 5)

      // Lock key located in node3 by request to node1
      (node1 ? client.Lock(transactionId, 8)) `should equal` client.Locked(transactionId, 8)

      // Check blocks
      // Only origin blocks should be locked
      (node1 ? system.GetBlockStatuses) `should equal`
        system.BlockStatuses(ArrayBuffer((2, WriteLock(transactionId)/*First lock*/), (5, Active), (4, Active), (1, Active), (3, Active), (0, Active)))

      (node2 ? system.GetBlockStatuses) `should equal`
        system.BlockStatuses(ArrayBuffer((8, Active), (5, WriteLock(transactionId)/*Second lock*/), (4, Active), (7, Active), (9, Active), (3, Active), (6, Active)))

      (node3 ? system.GetBlockStatuses) `should equal`
        system.BlockStatuses(ArrayBuffer((8, WriteLock(transactionId)/*Third lock*/), (2, Active), (7, Active), (1, Active), (9, Active), (6, Active), (0, Active)))


      // Client able to send message to any node
      // Node will rote the message to the appropriate node

      val fakeTransactionId = 2
      // Try to unlock key located in node1 by request to node1 with fake transaction id
      (node1 ? client.Unlock(fakeTransactionId, 2)) `should equal` client.ExternallyLocked(fakeTransactionId, 2)

      //  Try to unlock key located in node2 by request to node1 with fake transaction id
      (node1 ? client.Unlock(fakeTransactionId, 5)) `should equal` client.ExternallyLocked(fakeTransactionId, 5)

      //  Try to unlock key located in node3 by request to node1 with fake transaction id
      (node1 ? client.Unlock(fakeTransactionId, 8)) `should equal` client.ExternallyLocked(fakeTransactionId, 8)

      // Nothing should be changed
      (node1 ? system.GetBlockStatuses) `should equal`
        system.BlockStatuses(ArrayBuffer((2, WriteLock(transactionId)/*First lock*/), (5, Active), (4, Active), (1, Active), (3, Active), (0, Active)))

      (node2 ? system.GetBlockStatuses) `should equal`
        system.BlockStatuses(ArrayBuffer((8, Active), (5, WriteLock(transactionId)/*Second lock*/), (4, Active), (7, Active), (9, Active), (3, Active), (6, Active)))

      (node3 ? system.GetBlockStatuses) `should equal`
        system.BlockStatuses(ArrayBuffer((8, WriteLock(transactionId)/*Third lock*/), (2, Active), (7, Active), (1, Active), (9, Active), (6, Active), (0, Active)))



      // Unlock key located in node1 by request to node1
      (node1 ? client.Unlock(transactionId, 2)) `should equal` client.Unlocked(transactionId, 2)

      //  Unlock key located in node2 by request to node1
      (node1 ? client.Unlock(transactionId, 5)) `should equal` client.Unlocked(transactionId, 5)

      //  Unlock key located in node3 by request to node1
      (node1 ? client.Unlock(transactionId, 8)) `should equal` client.Unlocked(transactionId, 8)

      (node1 ? system.GetBlockStatuses) `should equal`
        system.BlockStatuses(ArrayBuffer((2, Active), (5, Active), (4, Active), (1, Active), (3, Active), (0, Active)))

      (node2 ? system.GetBlockStatuses) `should equal`
        system.BlockStatuses(ArrayBuffer((8, Active), (5, Active), (4, Active), (7, Active), (9, Active), (3, Active), (6, Active)))

      (node3 ? system.GetBlockStatuses) `should equal`
        system.BlockStatuses(ArrayBuffer((8, Active), (2, Active), (7, Active), (1, Active), (9, Active), (6, Active), (0, Active)))


      system3.terminate()
      system2.terminate()
      system1.terminate()

      Thread.sleep(60000)

    }
  }

  "A three-node cluster" must {

    "append records into the appropriate block" in {

      val group = 315
      val seed  = 1

      val system1 = ActorSystem("ClusterSystem", config(group, seed, 1))
      val partitions = 10
      val emptyBlock = Block(mutable.HashMap[Long, mutable.ListBuffer[String]]())
      val transactionId = 1
      implicit val timeout: Timeout = Timeout(5 seconds)

      val node1 = system1.actorOf(Props(new ClusterNode[String](partitions)), name = "node")

      Thread.sleep(20000)

      // Add node2 to the cluster
      val system2 = ActorSystem("ClusterSystem", config(group, seed, 2))

      val node2 = system2.actorOf(Props(new ClusterNode[String](partitions)), name = "node")

      Thread.sleep(20000)

      // Add node3 to the cluster
      val system3 = ActorSystem("ClusterSystem", config(group, seed, 3))

      val node3 = system3.actorOf(Props(new ClusterNode[String](partitions)), name = "node")

      Thread.sleep(20000)

      // Check rebalancing. Yet another time. I don't believe in this magic
      (node1 ? system.GetBlockTypes) `should equal`
        system.BlockTypes(ArrayBuffer((2, Original), (5, Recovery), (4, Recovery), (1, Original), (3, Recovery), (0, Original)))

      (node2 ? system.GetBlockTypes) `should equal`
        system.BlockTypes(ArrayBuffer((8, Recovery), (5, Original), (4, Original), (7, Recovery), (9, Recovery), (3, Original), (6, Recovery)))

      (node3 ? system.GetBlockTypes) `should equal`
        system.BlockTypes(ArrayBuffer((8, Original), (2, Recovery), (7, Original), (1, Recovery), (9, Original), (6, Original), (0, Recovery)))


      (node1 ? system.GetBlockStatuses) `should equal`
        system.BlockStatuses(ArrayBuffer((2, Active), (5, Active), (4, Active), (1, Active), (3, Active), (0, Active)))

      (node2 ? system.GetBlockStatuses) `should equal`
        system.BlockStatuses(ArrayBuffer((8, Active), (5, Active), (4, Active), (7, Active), (9, Active), (3, Active), (6, Active)))

      (node3 ? system.GetBlockStatuses) `should equal`
        system.BlockStatuses(ArrayBuffer((8, Active), (2, Active), (7, Active), (1, Active), (9, Active), (6, Active), (0, Active)))


      (node1 ? system.GetBlocks) `should equal`
        system.Blocks(ArrayBuffer((2, emptyBlock), (5, emptyBlock), (4, emptyBlock), (1, emptyBlock), (3, emptyBlock), (0, emptyBlock)))

      (node2 ? system.GetBlocks) `should equal`
        system.Blocks(ArrayBuffer((8, emptyBlock), (5, emptyBlock), (4, emptyBlock), (7, emptyBlock), (9, emptyBlock), (3, emptyBlock), (6, emptyBlock)))

      (node3 ? system.GetBlocks) `should equal`
        system.Blocks(ArrayBuffer((8, emptyBlock), (2, emptyBlock), (7, emptyBlock), (1, emptyBlock), (9, emptyBlock), (6, emptyBlock), (0, emptyBlock)))

      // Client able to send message to any node
      // Node will rote the message to the appropriate node

      // Try to append record to non-locked block in node2 by request to node1
      (node1 ? client.Append(transactionId, 2, "record #2")) `should equal` client.NonLockedAppend(transactionId, 2)

      // Try to append record to non-locked block in node2 by request to node1
      (node1 ? client.Append(transactionId, 5, "record #5")) `should equal` client.NonLockedAppend(transactionId, 5)

      // Try to append record to non-locked block in node2 by request to node1
      (node1 ? client.Append(transactionId, 8, "record #8")) `should equal` client.NonLockedAppend(transactionId, 8)

      // Nothing should be changed
      (node1 ? system.GetBlocks) `should equal`
        system.Blocks(ArrayBuffer((2, emptyBlock), (5, emptyBlock), (4, emptyBlock), (1, emptyBlock), (3, emptyBlock), (0, emptyBlock)))

      (node2 ? system.GetBlocks) `should equal`
        system.Blocks(ArrayBuffer((8, emptyBlock), (5, emptyBlock), (4, emptyBlock), (7, emptyBlock), (9, emptyBlock), (3, emptyBlock), (6, emptyBlock)))

      (node3 ? system.GetBlocks) `should equal`
        system.Blocks(ArrayBuffer((8, emptyBlock), (2, emptyBlock), (7, emptyBlock), (1, emptyBlock), (9, emptyBlock), (6, emptyBlock), (0, emptyBlock)))


      // Lock appropriate keys
      (node1 ? client.Lock(transactionId, 2)) `should equal` client.Locked(transactionId, 2)
      (node1 ? client.Lock(transactionId, 5)) `should equal` client.Locked(transactionId, 5)
      (node1 ? client.Lock(transactionId, 8)) `should equal` client.Locked(transactionId, 8)

      // Try to append record to locked block in node2 by request to node1
      (node1 ? client.Append(transactionId, 2, "record #2")) `should equal` client.Appended(transactionId, 2)

      // Try to append record to locked block in node2 by request to node1
      (node1 ? client.Append(transactionId, 5, "record #5")) `should equal` client.Appended(transactionId, 5)

      // Try to append record to locked block in node2 by request to node1
      (node1 ? client.Append(transactionId, 8, "record #8")) `should equal` client.Appended(transactionId, 8)

      // Check blocks for node 1
      (node1 ? system.GetBlocks) `should equal`
        Blocks(ArrayBuffer(
          (2, Block(mutable.HashMap(2L -> ListBuffer("record #2")))),
          (5, Block(mutable.HashMap(5L -> ListBuffer("record #5")))),
          (4, Block(mutable.HashMap[Long, ListBuffer[String]]())),
          (1, Block(mutable.HashMap[Long, ListBuffer[String]]())),
          (3, Block(mutable.HashMap[Long, ListBuffer[String]]())),
          (0, Block(mutable.HashMap[Long, ListBuffer[String]]()))
        ))

      // Check blocks for node 2
      (node2 ? system.GetBlocks) `should equal`
        Blocks(ArrayBuffer(
          (8, Block(mutable.HashMap(8L -> ListBuffer("record #8")))),
          (5, Block(mutable.HashMap(5L -> ListBuffer("record #5")))),
          (4, Block(mutable.HashMap[Long, ListBuffer[String]]())),
          (7, Block(mutable.HashMap[Long, ListBuffer[String]]())),
          (9, Block(mutable.HashMap[Long, ListBuffer[String]]())),
          (3, Block(mutable.HashMap[Long, ListBuffer[String]]())),
          (6, Block(mutable.HashMap[Long, ListBuffer[String]]()))
        ))

      // Check blocks for node 3
      (node3 ? system.GetBlocks) `should equal`
        Blocks(ArrayBuffer(
          (8, Block(mutable.HashMap(8L -> ListBuffer("record #8")))),
          (2, Block(mutable.HashMap(2L -> ListBuffer("record #2")))),
          (7, Block(mutable.HashMap[Long, ListBuffer[String]]())),
          (1, Block(mutable.HashMap[Long, ListBuffer[String]]())),
          (9, Block(mutable.HashMap[Long, ListBuffer[String]]())),
          (6, Block(mutable.HashMap[Long, ListBuffer[String]]())),
          (0, Block(mutable.HashMap[Long, ListBuffer[String]]()))
        ))

      // Try to append blocks with fake transaction
      val fakeTransactionId = 42

      // Try to append record to non-locked block in node2 by request to node1
      (node1 ? client.Append(fakeTransactionId, 2, "record #2")) `should equal` client.ExternallyLocked(fakeTransactionId, 2)

      // Try to append record to non-locked block in node2 by request to node1
      (node1 ? client.Append(fakeTransactionId, 5, "record #5")) `should equal` client.ExternallyLocked(fakeTransactionId, 5)

      // Try to append record to non-locked block in node2 by request to node1
      (node1 ? client.Append(fakeTransactionId, 8, "record #8")) `should equal` client.ExternallyLocked(fakeTransactionId, 8)

      // Nothing should be changed
      // Check blocks for node 1
      (node1 ? system.GetBlocks) `should equal`
        Blocks(ArrayBuffer(
          (2, Block(mutable.HashMap(2L -> ListBuffer("record #2")))),
          (5, Block(mutable.HashMap(5L -> ListBuffer("record #5")))),
          (4, Block(mutable.HashMap[Long, ListBuffer[String]]())),
          (1, Block(mutable.HashMap[Long, ListBuffer[String]]())),
          (3, Block(mutable.HashMap[Long, ListBuffer[String]]())),
          (0, Block(mutable.HashMap[Long, ListBuffer[String]]()))
        ))

      // Check blocks for node 2
      (node2 ? system.GetBlocks) `should equal`
        Blocks(ArrayBuffer(
          (8, Block(mutable.HashMap(8L -> ListBuffer("record #8")))),
          (5, Block(mutable.HashMap(5L -> ListBuffer("record #5")))),
          (4, Block(mutable.HashMap[Long, ListBuffer[String]]())),
          (7, Block(mutable.HashMap[Long, ListBuffer[String]]())),
          (9, Block(mutable.HashMap[Long, ListBuffer[String]]())),
          (3, Block(mutable.HashMap[Long, ListBuffer[String]]())),
          (6, Block(mutable.HashMap[Long, ListBuffer[String]]()))
        ))

      // Check blocks for node 3
      (node3 ? system.GetBlocks) `should equal`
        Blocks(ArrayBuffer(
          (8, Block(mutable.HashMap(8L -> ListBuffer("record #8")))),
          (2, Block(mutable.HashMap(2L -> ListBuffer("record #2")))),
          (7, Block(mutable.HashMap[Long, ListBuffer[String]]())),
          (1, Block(mutable.HashMap[Long, ListBuffer[String]]())),
          (9, Block(mutable.HashMap[Long, ListBuffer[String]]())),
          (6, Block(mutable.HashMap[Long, ListBuffer[String]]())),
          (0, Block(mutable.HashMap[Long, ListBuffer[String]]()))
        ))

      system3.terminate()
      system2.terminate()
      system1.terminate()

      Thread.sleep(60000)

    }
  }

  "A three-node cluster" must {

    "read records from the appropriate block" in {

      val group = 335
      val seed  = 1

      val system1 = ActorSystem("ClusterSystem", config(group, seed, 1))
      val partitions = 10
      val emptyBlock = Block(mutable.HashMap[Long, mutable.ListBuffer[String]]())
      val transactionId = 1
      val anotherTransactionId = 2
      implicit val timeout: Timeout = Timeout(5 seconds)

      val node1 = system1.actorOf(Props(new ClusterNode[String](partitions)), name = "node")

      Thread.sleep(20000)

      // Add node2 to the cluster
      val system2 = ActorSystem("ClusterSystem", config(group, seed, 2))

      val node2 = system2.actorOf(Props(new ClusterNode[String](partitions)), name = "node")

      Thread.sleep(20000)

      // Add node3 to the cluster
      val system3 = ActorSystem("ClusterSystem", config(group, seed, 3))

      val node3 = system3.actorOf(Props(new ClusterNode[String](partitions)), name = "node")

      Thread.sleep(20000)

      // Check rebalancing. Yet another time. I don't believe in this magic
      (node1 ? system.GetBlockTypes) `should equal`
        system.BlockTypes(ArrayBuffer((2, Original), (5, Recovery), (4, Recovery), (1, Original), (3, Recovery), (0, Original)))

      (node2 ? system.GetBlockTypes) `should equal`
        system.BlockTypes(ArrayBuffer((8, Recovery), (5, Original), (4, Original), (7, Recovery), (9, Recovery), (3, Original), (6, Recovery)))

      (node3 ? system.GetBlockTypes) `should equal`
        system.BlockTypes(ArrayBuffer((8, Original), (2, Recovery), (7, Original), (1, Recovery), (9, Original), (6, Original), (0, Recovery)))


      (node1 ? system.GetBlockStatuses) `should equal`
        system.BlockStatuses(ArrayBuffer((2, Active), (5, Active), (4, Active), (1, Active), (3, Active), (0, Active)))

      (node2 ? system.GetBlockStatuses) `should equal`
        system.BlockStatuses(ArrayBuffer((8, Active), (5, Active), (4, Active), (7, Active), (9, Active), (3, Active), (6, Active)))

      (node3 ? system.GetBlockStatuses) `should equal`
        system.BlockStatuses(ArrayBuffer((8, Active), (2, Active), (7, Active), (1, Active), (9, Active), (6, Active), (0, Active)))


      (node1 ? system.GetBlocks) `should equal`
        system.Blocks(ArrayBuffer((2, emptyBlock), (5, emptyBlock), (4, emptyBlock), (1, emptyBlock), (3, emptyBlock), (0, emptyBlock)))

      (node2 ? system.GetBlocks) `should equal`
        system.Blocks(ArrayBuffer((8, emptyBlock), (5, emptyBlock), (4, emptyBlock), (7, emptyBlock), (9, emptyBlock), (3, emptyBlock), (6, emptyBlock)))

      (node3 ? system.GetBlocks) `should equal`
        system.Blocks(ArrayBuffer((8, emptyBlock), (2, emptyBlock), (7, emptyBlock), (1, emptyBlock), (9, emptyBlock), (6, emptyBlock), (0, emptyBlock)))

      // Client able to send message to any node
      // Node will rote the message to the appropriate node

      // Lock appropriate keys
      (node1 ? client.Lock(transactionId, 2)) `should equal` client.Locked(transactionId, 2)
      (node1 ? client.Lock(transactionId, 5)) `should equal` client.Locked(transactionId, 5)
      (node1 ? client.Lock(transactionId, 8)) `should equal` client.Locked(transactionId, 8)

      (node1 ? client.Append(transactionId, 2, "record #2")) `should equal` client.Appended(transactionId, 2)
      (node1 ? client.Append(transactionId, 5, "record #5")) `should equal` client.Appended(transactionId, 5)
      (node1 ? client.Append(transactionId, 8, "record #8")) `should equal` client.Appended(transactionId, 8)

      // Read locked records
      (node1 ? client.Read(transactionId, 2)) `should equal`
        client.Records(transactionId, 2, records = Seq[String](s"record #2"))

      (node1 ? client.Read(transactionId, 5)) `should equal`
        client.Records(transactionId, 5, records = Seq[String](s"record #5"))

      (node1 ? client.Read(transactionId, 8)) `should equal`
        client.Records(transactionId, 8, records = Seq[String](s"record #8"))


      // Read records with another transaction
      (node1 ? client.Read(anotherTransactionId, 2)) `should equal`
        client.Records(anotherTransactionId, 2, records = Seq[String](s"record #2"))

      (node1 ? client.Read(anotherTransactionId, 5)) `should equal`
        client.Records(anotherTransactionId, 5, records = Seq[String](s"record #5"))

      (node1 ? client.Read(anotherTransactionId, 8)) `should equal`
        client.Records(anotherTransactionId, 8, records = Seq[String](s"record #8"))


      // Unlock appropriate keys
      (node1 ? client.Unlock(transactionId, 2)) `should equal` client.Unlocked(transactionId, 2)
      (node1 ? client.Unlock(transactionId, 5)) `should equal` client.Unlocked(transactionId, 5)
      (node1 ? client.Unlock(transactionId, 8)) `should equal` client.Unlocked(transactionId, 8)

      // Try to read non-locked records
      (node1 ? client.Read(transactionId, 2)) `should equal`
        client.Records(transactionId, 2, records = Seq[String](s"record #2"))

      (node1 ? client.Read(transactionId, 5)) `should equal`
        client.Records(transactionId, 5, records = Seq[String](s"record #5"))

      (node1 ? client.Read(transactionId, 8)) `should equal`
        client.Records(transactionId, 8, records = Seq[String](s"record #8"))


      system3.terminate()
      system2.terminate()
      system1.terminate()

      Thread.sleep(60000)

    }
  }

}

