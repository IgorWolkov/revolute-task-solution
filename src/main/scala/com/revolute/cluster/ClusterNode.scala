package com.revolute.cluster

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSelection, Address, RootActorPath}
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, Member}
import akka.pattern.ask
import akka.util.Timeout
import model._
import model.client.ClientMessage
import model.system.SystemMessage

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps

class ClusterNode[T](partitions: Int) extends Actor with ActorLogging {

  val cluster = Cluster(context.system)

  var blockStatuses: mutable.HashMap[Int, BlockStatus] = mutable.HashMap[Int, BlockStatus]()
  var blockTypes: mutable.HashMap[Int, BlockType] = mutable.HashMap[Int, BlockType]()
  var blocks: mutable.HashMap[Int, Block[T]] = mutable.HashMap[Int, Block[T]]()

  var rebalancingBlockStatuses: mutable.HashMap[Int, BlockStatus] = mutable.HashMap[Int, BlockStatus]()
  var rebalancingBlockTypes: mutable.HashMap[Int, BlockType] = mutable.HashMap[Int, BlockType]()
  var rebalancingBlocks: mutable.HashMap[Int, Block[T]] = mutable.HashMap[Int, Block[T]]()

  var memberAddresses: List[Address] = List[Address]()

  var rebalancingMemberAddresses: List[Address] = List[Address]()
  var rebalancingMembersCounter = 0
  var rebalancingNeeded = false

  var rebalancingActiveIndexes: Seq[Int] = Seq[Int]()
  var rebalancingRecoveryIndexes: Seq[Int] = Seq[Int]()

  var nodeStatus: NodeStatus = Starting

  // Add magic number to the config
  implicit val timeout: Timeout = Timeout(10 seconds)
  import context.dispatcher

  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def postStop(): Unit = cluster.unsubscribe(self)

  override def receive: Receive = {

    case clientMessage: ClientMessage => handleClientMessage(sender(), clientMessage)

    case systemMessage: SystemMessage => handleSystemMessage(sender(), systemMessage)

    case MemberUp(member) =>
      log info s"Member [$member] joined the cluster"
      startRebalancing()

    case MemberRemoved(leaver, _) =>
      log info s"Member [$leaver] left the cluster. Current cluster members: ${cluster.state.members map { _.address } toList}"
      removeMember(leaver)
  }

  def handleClientMessage(actor: ActorRef, message: model.client.ClientMessage): Unit = {

    message match {
      case client.Lock(transactionId, key) =>
        log debug s"Lock request for transaction id [$transactionId] and key [$key]"
        lockBlockByKey(actor, transactionId, key)

      case client.Unlock(transactionId, key) =>
        log debug s"Unlock request for transaction id [$transactionId] and key [$key]"
        unlockBlockByKey(actor, transactionId, key)

      case client.Append(transactionId, key, record: T) =>
        log debug s"Append request for transaction id [$transactionId] and key [$key]"
        appendByKey(actor, transactionId, key, record)

      case client.Read(transactionId, key) =>
        log error s"Read request for transaction id [$transactionId] and key [$key]"
        readByKey(actor, transactionId, key)
    }

    def lockBlockByKey(actor: ActorRef, transactionId: Long, key: Long): Unit = {
      val index = calculateIndexByKey(key)
      val activeNode = calculateActiveNodeAddressByKey(key)

      (actorBy(activeNode) ? system.WriteLock(index, transactionId)) map {
        case system.WriteLocked(`index`, `transactionId`) =>
          log debug s"Transaction [$transactionId]. Key [$key] was successfully locked"
          actor ! client.Locked(transactionId, key)

        case error: SystemMessage =>
          log error s"Transaction [$transactionId]. Cannot lock [$key]. Cause: [$error]"
          handleError(actor, error, key)
      }
    }

    def unlockBlockByKey(actor: ActorRef, transactionId: Long, key: Long): Unit = {
      val index = calculateIndexByKey(key)
      val activeNode = calculateActiveNodeAddressByKey(key)

      (actorBy(activeNode) ? system.WriteUnlock(index, transactionId)) map {
        case system.WriteUnlocked(`index`, `transactionId`) =>
          log debug s"Transaction [$transactionId]. Key [$key] was successfully unlocked"
          actor ! client.Unlocked(transactionId, key)

        case error: SystemMessage =>
          log error s"Transaction [$transactionId]. Cannot unlock [$key]. Cause: [$error]"
          handleError(actor, error, key)
      }
    }

    def appendByKey(actor: ActorRef, transactionId: Long, key: Long, record: T): Unit = {
      val index = calculateIndexByKey(key)
      val activeNode = calculateActiveNodeAddressByKey(key)
      val recoveryNode = calculateRecoveryNodeAddressByKey(key)

      (actorBy(activeNode) ? system.Append(index, transactionId, key, record)) map {
        case system.Appended(`index`, `transactionId`, `key`) if activeNode == recoveryNode =>
          log debug s"Transaction [$transactionId]. Record [$record] was successfully appended to key [$key] on active node only"
          actor ! client.Appended(transactionId, key)

        case system.Appended(`index`, `transactionId`, `key`) if activeNode != recoveryNode  =>

          (actorBy(recoveryNode) ? system.Append(index, transactionId, key, record)) foreach {
            case system.Appended(_, _, _) =>
              log debug s"Transaction [$transactionId]. Record [$record] was successfully appended to key [$key] on active and recovery node"

            case error: SystemMessage =>
              log warning s"Transaction [$transactionId]. Error while appending record [$record] to key [$key]. Cause: [$error]"
          }

          actor ! client.Appended(transactionId, key)

        case error: SystemMessage =>
          log error s"Transaction [$transactionId]. Cannot append record [$record] to key [$key]. Cause: [$error]"
          handleError(actor, error, key)
      }
    }

    def readByKey(actor: ActorRef, transactionId: Long, key: Long): Unit = {
      val index = calculateIndexByKey(key)
      val activeNode = calculateActiveNodeAddressByKey(key)
      val recoveryNode = calculateRecoveryNodeAddressByKey(key)

      (actorBy(activeNode) ? system.Read(index, transactionId, key)) map {
        case system.Records(`index`, `transactionId`, `key`, records) =>
          log debug s"Transaction [$transactionId]. Records [$records] was successfully read from key [$key] from active node"
          actor ! client.Records(transactionId, key, records)

        case system.NotFound(_, _, _) =>
          (actorBy(recoveryNode) ? system.Read(index, transactionId, key)) map {
            case system.Records(`index`, `transactionId`, `key`, records) =>
              log warning s"Transaction [$transactionId]. Records [$records] was read from key [$key] from recovery node"
              actor ! client.Records(transactionId, key, records)

            case error: SystemMessage =>
              log error s"Transaction [$transactionId]. Cannot read records from key [$key]. Cause: [$error]"
              handleError(actor, error, key)
          }

        case error: SystemMessage =>
          log error s"Transaction [$transactionId]. Cannot read records from key [$key]. Cause: [$error]"
          handleError(actor, error, key)
      }
    }

    def handleError(actor: ActorRef, error: SystemMessage, key: Long): Unit =
      error match {
        case system.ExternallyLocked(_, transactionId)    => actor ! client.ExternallyLocked(transactionId, key)
        case system.BrokenIndex(_, transactionId)         => actor ! client.BrokenKey(transactionId, key)
        case system.NotFound(_, transactionId, _)         => actor ! client.NotFound(transactionId, key)
        case system.NonLockedAppend(_, transactionId, _)  => actor ! client.NonLockedAppend(transactionId, key)
        case system.NonLockedRead(_, transactionId, _)    => actor ! client.NonLockedRead(transactionId, key)
      }

    def calculateIndexByKey(key: Long): Int = (key % partitions).toInt

    def calculateActiveNodeAddressByIndex(index: Int): Address =
      memberAddresses(index * memberAddresses.length / partitions)

    def calculateActiveNodeAddressByKey(key: Long): Address =
      calculateActiveNodeAddressByIndex(calculateIndexByKey(key))

    def calculateRecoveryNodeAddressByIndex(index: Int): Address = {
      val n = memberAddresses.length

      memberAddresses((index * n / partitions + 1) % n)
    }

    def calculateRecoveryNodeAddressByKey(key: Long): Address =
      calculateRecoveryNodeAddressByIndex(calculateIndexByKey(key))
  }


  def handleSystemMessage(actor: ActorRef, systemMessage: SystemMessage): Unit = {
    systemMessage match {

      case system.GetNodeStatus =>
        actor ! nodeStatus

      // Blocks' info
      // ==================================================
      case m @ system.GetBlockStatuses =>
        log debug s"System message: [$m]. Current block statuses: [${blockStatuses mkString ", "}]"
        actor ! system.BlockStatuses(blockStatuses.toSeq)

      case m @ system.GetBlockTypes =>
        log debug s"System message: [$m]. Current block types: [${blockTypes mkString ", "}]"
        actor ! system.BlockTypes(blockTypes.toSeq)

      case m @ system.GetBlocks =>
        log debug s"System message: [$m]. Current blocks: [${blocks mkString ", "}]"
        actor ! system.Blocks(blocks.toSeq)
      // ==================================================

      // Locks
      // ==================================================
      case m @ system.WriteLock(index: Int, transactionId: Long) =>
        log debug s"System message: [$m]. Current block statuses: [${blockStatuses mkString ", "}]"
        lockBlock(actor, index, transactionId)

      case m @ system.WriteUnlock(index: Int, transactionId: Long) =>
        log debug s"System message: [$m]. Current block statuses: [${blockStatuses mkString ", "}]"
        unlockBlock(actor, index, transactionId)
      // ==================================================

      // Writes
      // ==================================================
      case m @ system.Append(index: Int, transactionId: Long, key: Long, record: T) =>
        log debug s"System message: [$m]. Current blocks: [${blocks mkString ", "}]"
        appendRecord(actor, index, transactionId, key, record)
      // ==================================================

      // Reads
      // ==================================================
      case m @ system.Read(index: Int, transactionId: Long, key: Long) =>
        log debug s"System message: [$m]. Current blocks: [${blocks mkString ", "}]"
        readRecords(actor, index, transactionId, key)
      // ==================================================


      // Balancing: general
      // ==================================================
      case m @ system.StartRebalancing(addresses) =>
        log info s"System message: [$m]"
        rebalancingNeeded = false
        nodeStatus = Rebalancing
        rebalancingMemberAddresses = addresses
        rebalancingMembersCounter = addresses.size
        val rebalancingIndex = rebalancingMemberAddresses.indexOf(cluster.selfAddress)
        val rebalancingMembersSize = rebalancingMemberAddresses.size

        rebalancingActiveIndexes = calculateRebalancingActiveIndexes(rebalancingIndex, rebalancingMembersSize)
        rebalancingRecoveryIndexes = calculateRebalancingRecoveryIndexes(rebalancingIndex, rebalancingMembersSize)

        sendTo(addresses, system.GetOriginalCopyForOriginal(rebalancingActiveIndexes))
        sendTo(addresses, system.GetOriginalCopyForRecovery(rebalancingRecoveryIndexes))

      case m @ system.CommitRebalancing =>
        log info s"System message: [$m]"
        nodeStatus = Running
        blockStatuses = rebalancingBlockStatuses
        blockTypes = rebalancingBlockTypes
        blocks = rebalancingBlocks
        rebalancingBlockStatuses = mutable.HashMap[Int, BlockStatus]()
        rebalancingBlockTypes = mutable.HashMap[Int, BlockType]()
        rebalancingBlocks = mutable.HashMap[Int, Block[T]]()
        memberAddresses = rebalancingMemberAddresses

      case m @ system.RebalancingFinished =>
        log debug s"System message: [$m]"
        if(nodeIsLeader) {
          rebalancingMembersCounter -= 1
          log info s"Node [${actor.path.address}] finished rebalancing"

          if(rebalancingMembersCounter == 0) {
            log info s"All nodes [${actor.path.address}] finished rebalancing. Commit rebalancing for [${rebalancingMemberAddresses mkString ", "}]"
            sendTo(rebalancingMemberAddresses, system.CommitRebalancing)
          }
        }
      // ==================================================

      // Balancing: copy and recovery
      // ==================================================
      case m @ system.GetOriginalCopyForOriginal(indexes) =>
        log debug s"System message: [$m]. Current blocks statuses: [${blockStatuses mkString ", "}]"
        processOriginalCopyRequest(actor, indexes, (index, block) => system.OriginalCopyForOriginal(index, block))

      case m @ system.GetOriginalCopyForRecovery(indexes) =>
        log debug s"System message: [$m]. Current blocks statuses: [${blockStatuses mkString ", "}]"
        processOriginalCopyRequest(actor, indexes, (index, block) => system.OriginalCopyForRecovery(index, block))

      case msg: system.OriginalCopyForOriginal[T] =>
        log debug s"System message: [$msg]. Current blocks statuses: [${blockStatuses mkString ", "}]"
        processOriginalCopyResponse(actor, msg.index, msg.block, Original)

      case msg: system.OriginalCopyForRecovery[T] =>
        log debug s"System message: [$msg]. Current blocks statuses: [${blockStatuses mkString ", "}]"
        processOriginalCopyResponse(actor, msg.index, msg.block, Recovery)
      // ==================================================


      // Balancing: recovery
      // ==================================================
      case m @ system.PromoteToOrigin(indexes) =>
        log debug s"System message: [$m]. Promote recovery indexes to origins"
        indexes map { index =>
          if(blockTypes.contains(index))
            blockTypes.put(index, Original)
        }
      // ==================================================

    }

    def calculateRebalancingRecoveryIndexes(rebalancingIndex: Int, rebalancingMembersSize: Int): Seq[Int] = {
      val recoveryPartitionsFrom = ((rebalancingIndex + 1) % rebalancingMembersSize) * partitions / rebalancingMembersSize
      val recoveryPartitionsTo = ((rebalancingIndex + 1) % rebalancingMembersSize + 1) * partitions / rebalancingMembersSize - 1

      recoveryPartitionsFrom to recoveryPartitionsTo
    }

    def processOriginalCopyRequest(actor: ActorRef, indexes: Seq[Int], m: (Int, Block[T]) => Message): Unit = {
      indexes foreach { index =>
        (blockTypes.get(index) filter { _ == Original}) foreach {_ =>
          blocks.get(index) foreach { block =>
            blockStatuses.put(index, RebalancingWriteLock)
            actor ! m(index, block)
          }
        }
      }
    }

    def processOriginalCopyResponse(actor: ActorRef, index: Int, block: Block[T], blockType: BlockType): Unit = {
      rebalancingBlockStatuses.put(index, Active)
      rebalancingBlockTypes.put(index, blockType)
      rebalancingBlocks.put(index, block)

      if((rebalancingActiveIndexes forall { rebalancingBlockStatuses.contains }) &&
        (rebalancingRecoveryIndexes forall { rebalancingBlockStatuses.contains })) {
        cluster.state.leader foreach {
          sendTo(_, system.RebalancingFinished)
        }
      }
    }

    def appendRecord(actor: ActorRef, index: Int, transactionId: Long, key: Long, record: T): Unit = {
      blockStatuses.get(index) match {
        case Some(WriteLock(`transactionId`)) =>
          append(actor, index, transactionId, key, record)

        case Some(WriteLock(_)) | Some(Active) if blockTypes.get(index).contains(Recovery) =>
          append(actor, index, transactionId, key, record)

        case Some(Active)               => actor ! system.NonLockedAppend(index, transactionId, key)
        case Some(RebalancingWriteLock) => actor ! system.ExternallyLocked(index, transactionId)
        case Some(WriteLock(_))         => actor ! system.ExternallyLocked(index, transactionId)
        case None                       => actor ! system.NotFound(index, transactionId, key)

      }

      def append(actor: ActorRef, index: Int, transactionId: Long, key: Long, record: T): Unit =
        blocks.get(index) match {
          case Some(block) =>
            if(!block.data.contains(key))
              block.data.put(key, mutable.ListBuffer[T](record))
            else
              block.data(key).append(record)

            actor ! system.Appended(index, transactionId, key)

          case None =>
            blockStatuses.remove(index)
            actor ! system.NotFound(index, transactionId, key)
        }
    }

    def readRecords(actor: ActorRef, index: Int, transactionId: Long, key: Long): Unit = {
      blockStatuses.get(index) match {
        case Some(_) =>
          read(actor, index, transactionId, key)

        case None =>
          actor ! system.NotFound(index, transactionId, key)

      }

      def read(actor: ActorRef, index: Int, transactionId: Long, key: Long): Unit =
        blocks.get(index) match {
          case Some(block) =>
            if(!block.data.contains(key))
              actor ! system.NotFound(index, transactionId, key)
            else
              actor ! system.Records(index, transactionId, key, block.data(key))

          case None =>
            blockStatuses.remove(index)
            actor ! system.NotFound(index, transactionId, key)
        }
    }

    def lockBlock(actor: ActorRef, index: Int, transactionId: Long): Unit =
      blockStatuses.get(index) match {
        case Some(_) if nodeStatus == Rebalancing =>
          actor ! system.ExternallyLocked(index, transactionId)

        case Some(Active) =>
          blockStatuses.put(index, WriteLock(transactionId))
          actor ! system.WriteLocked(index, transactionId)

        case Some(WriteLock(`transactionId`)) =>
          actor ! system.WriteLocked(index, transactionId)

        case Some(WriteLock(_)) =>
          actor ! system.ExternallyLocked(index, transactionId)

        case Some(RebalancingWriteLock) =>
          actor ! system.ExternallyLocked(index, transactionId)

        case None =>
          actor ! system.BrokenIndex(index, transactionId)
      }

    def unlockBlock(actor: ActorRef, index: Int, transactionId: Long): Unit =
      blockStatuses.get(index) match {
        case Some(WriteLock(`transactionId`) | Active) =>
          blockStatuses.put(index, Active)
          actor ! system.WriteUnlocked(index, transactionId)

        case Some(_) =>
          actor ! system.ExternallyLocked(index, transactionId)

        case None =>
          actor ! system.BrokenIndex(index, transactionId)
      }
  }

  def startRebalancing(): Unit = {
    // If it's already rebalancing
    if (nodeIsLeader) {
      log info s"Current status: $nodeStatus. Leader: ${cluster.state.leader}"
      if (nodeStatus == Rebalancing) {
        // Don't forget start another rebalancing and set it to false
        log info s"Postpone rebalancing because the one has started"
        rebalancingNeeded = true
      } else {
        val addresses = collectAddresses


        if(addresses.length < 2 && nodeStatus == Starting) {
          log info s"First node in the cluster was initiated"
          (0 until partitions) foreach { index =>
            blockStatuses.put(index, Active)
            blockTypes.put(index, Original)
            blocks.put(index, Block[T](data = mutable.HashMap[Long, mutable.ListBuffer[T]]()))
          }
          memberAddresses = collectAddresses
          nodeStatus = Running
        } else {
          sendTo(addresses, system.StartRebalancing(addresses))
          log info s"Start rebalancing for " + (addresses mkString ", ")
        }

      }
    }
  }

  def removeMember(leaver: Member): Unit = {
    if (nodeIsLeader) {
      val leaverIndex = memberAddresses.indexOf(leaver.address)

      val promotingIndexes = calculateRebalancingActiveIndexes(leaverIndex, memberAddresses.size)

      memberAddresses = memberAddresses filter {
        _ != leaver.address
      }

      log info s"Member [$leaver] was removed from the cluster. Affected indexes: [$promotingIndexes]"

      sendTo(memberAddresses, system.PromoteToOrigin(promotingIndexes))

      log info s"Start rebalancing after member [$leaver] was removed"
      startRebalancing()
    }
  }

  def calculateRebalancingActiveIndexes(rebalancingIndex: Int, rebalancingMembersSize: Int): Seq[Int] = {
    val activePartitionsFrom = rebalancingIndex * partitions / rebalancingMembersSize
    val activePartitionsTo = (rebalancingIndex + 1) * partitions / rebalancingMembersSize - 1

    activePartitionsFrom to activePartitionsTo
  }

  def nodeIsLeader: Boolean =
    cluster.state.leader contains cluster.selfMember.address

  def collectAddresses: List[Address] =
    cluster.state.members map { _.address } toList

  def actorBy(address: Address): ActorSelection =
    cluster.system.actorSelection(RootActorPath(address) / "user" / "node")

  def sendTo(address: Address, message: Any): Unit =
    cluster.system.actorSelection(RootActorPath(address) / "user" / "node") ! message

  def sendTo(addresses: List[Address], message: Any): Unit =
    addresses.foreach { sendTo(_, message) }
}