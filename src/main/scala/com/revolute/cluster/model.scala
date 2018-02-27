package com.revolute.cluster

import akka.actor.Address

import scala.collection.mutable

object model {

  sealed trait TransactionType
  case object New extends TransactionType
  case object Committed extends TransactionType
  case object Reverted extends TransactionType

  case class Transaction(id: Long,
                         accountId: Long,
                         amount: Long,
                         associatedWith: Option[Long],
                         description: String,
                         `type`: TransactionType)

  case class TransactionBalance(accountId: Long, balance: Long)


  case class BankTransfer(from: Long, to: Long, amount: Long, description: String)
  case class AccountInitiation(accountId: Long, amount: Long, description: String)

  case class Block[T](data: mutable.HashMap[Long, mutable.ListBuffer[T]])

  sealed trait NodeStatus
  case object Starting extends NodeStatus
  case object Running extends NodeStatus
  case object Rebalancing extends NodeStatus

  sealed trait BlockStatus
  case object Active extends BlockStatus
  case object RebalancingWriteLock extends BlockStatus
  case class ReadWriteLock(transactionId: Long) extends BlockStatus
  case class WriteLock(transactionId: Long) extends BlockStatus

  sealed trait BlockType
  case object Original extends BlockType
  case object Recovery extends BlockType

  trait Message

  object external {
    sealed trait Calculations {
      val accountId: Long
    }
    case class Balance(accountId: Long, balance: Long) extends Calculations
    case class CorruptedBalance(accountId: Long, cause: String) extends Calculations

    sealed trait ExternalMessage extends Message
    sealed trait SuccessMessage extends ExternalMessage
    sealed trait FailureMessage extends ExternalMessage

    case class InitAccount(accountInitiation: AccountInitiation) extends ExternalMessage
    case class AccountInitiationComplete(accountInitiation: AccountInitiation) extends SuccessMessage
    case class AccountInitiationFailed(accountInitiation: AccountInitiation, cause: String) extends FailureMessage

    case class PerformBankTransfer(bankTransfer: BankTransfer) extends ExternalMessage
    case class BankTransferComplete(bankTransfer: BankTransfer) extends SuccessMessage
    case class BankTransferFailed(bankTransfer: BankTransfer, cause: String) extends FailureMessage

    case class RevertBankTransfer(transactionId: Long, bankTransfer: BankTransfer) extends ExternalMessage
    case class BankTransferReversionComplete(transactionId: Long, bankTransfer: BankTransfer) extends SuccessMessage
    case class BankTransferReversionFailed(transactionId: Long, bankTransfer: BankTransfer, cause: String) extends FailureMessage

    case class GetRecords(accountId: Long) extends ExternalMessage
    case class RecordsAndBalance(accountId: Long, balance: Calculations, records: Seq[Transaction]) extends SuccessMessage
    case class RecordsAndBalanceFailed(accountId: Long, cause: String) extends FailureMessage
  }


  object client {

    trait ClientMessage extends Message {
      val transactionId: Long
      val key: Long
    }

    // General errors
    case class BrokenKey(transactionId: Long, key: Long) extends ClientMessage
    case class NotFound(transactionId: Long, key: Long) extends ClientMessage

    // Locks
    case class Lock(transactionId: Long, key: Long) extends ClientMessage
    case class Locked(transactionId: Long, key: Long) extends ClientMessage
    case class Unlock(transactionId: Long, key: Long) extends ClientMessage
    case class Unlocked(transactionId: Long, key: Long) extends ClientMessage
    case class ExternallyLocked(transactionId: Long, key: Long) extends ClientMessage

    // Writes
    case class Append[T](transactionId: Long, key: Long, record: T) extends ClientMessage
    case class Appended(transactionId: Long, key: Long) extends ClientMessage
    case class NonLockedAppend(transactionId: Long, key: Long) extends ClientMessage

    // Reads
    case class Read[T](transactionId: Long, key: Long) extends ClientMessage
    case class Records[T](transactionId: Long, key: Long, records: Seq[T]) extends ClientMessage
    case class NonLockedRead(transactionId: Long, key: Long) extends ClientMessage
  }

  object system {

    trait SystemMessage extends Message

    case object GetNodeStatus extends SystemMessage

    // Blocks' info
    case object GetBlockStatuses extends SystemMessage
    case class BlockStatuses(statuses: Seq[(Int, BlockStatus)]) extends SystemMessage
    case object GetBlockTypes extends SystemMessage
    case class BlockTypes(types: Seq[(Int, BlockType)]) extends SystemMessage
    case object GetBlocks extends SystemMessage
    case class Blocks[T](blocks: Seq[(Int, Block[T])]) extends SystemMessage

    // Locks
    case class WriteLock(index: Int, transactionId: Long) extends SystemMessage
    case class WriteLocked(index: Int, transactionId: Long) extends SystemMessage
    case class WriteUnlock(index: Int, transactionId: Long) extends SystemMessage
    case class WriteUnlocked(index: Int, transactionId: Long) extends SystemMessage
    case class ExternallyLocked(index: Int, transactionId: Long) extends SystemMessage

    // Writes
    case class Append[T](index: Int, transactionId: Long, key: Long, record: T) extends SystemMessage
    case class Appended(index: Int, transactionId: Long, key: Long) extends SystemMessage
    case class NonLockedAppend(index: Int, transactionId: Long, key: Long) extends SystemMessage

    // Readings
    case class Read[T](index: Int, transactionId: Long, key: Long) extends SystemMessage
    case class Records[T](index: Int, transactionId: Long, key: Long, records: Seq[T]) extends SystemMessage
    case class NonLockedRead(index: Int, transactionId: Long, key: Long) extends SystemMessage

    // General errors
    case class BrokenIndex(index: Int, transactionId: Long) extends SystemMessage
    case class NotFound(index: Int, transactionId: Long, key: Long) extends SystemMessage

    // Balancing: general
    case class StartRebalancing(addresses: List[Address]) extends SystemMessage
    case object CommitRebalancing extends SystemMessage
    case object RebalancingFinished extends SystemMessage

    // Balancing: copy and recovery
    case class GetOriginalCopyForOriginal(indexes: Seq[Int]) extends SystemMessage
    case class OriginalCopyForOriginal[T](index: Int, block: Block[T]) extends SystemMessage
    case class GetOriginalCopyForRecovery(indexes: Seq[Int]) extends SystemMessage
    case class OriginalCopyForRecovery[T](index: Int, block: Block[T]) extends SystemMessage

    // Balancing: recovery
    case class PromoteToOrigin(indexes: Seq[Int]) extends SystemMessage
  }


}
