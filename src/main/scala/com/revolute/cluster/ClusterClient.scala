package com.revolute.cluster

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.pattern.ask
import akka.util.Timeout
import cats.data.EitherT
import model._
import monix.eval.Task

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random
import scala.util.control.NonFatal


object ClusterClient {

  trait BusinessEntity
  case object Locked extends BusinessEntity
  type Locked = Locked.type

  case object Unlocked extends BusinessEntity
  type Unlocked = Unlocked.type

  case class Records(accountId: Long, records: Seq[Transaction]) extends BusinessEntity

  case object BankTransferApproved extends BusinessEntity
  type BankTransferApproved = BankTransferApproved.type

  case object TransactionApplied extends BusinessEntity
  type TransactionApplied = TransactionApplied.type

  case object TransactionReverted extends BusinessEntity
  type TransactionReverted = TransactionReverted.type

  trait BusinessError {
    val transactionId: Long
    val account: Long
    val cause: String
  }

  trait BusinessErrors

  case class UnableToLock(transactionId: Long, account: Long, cause: String) extends BusinessError
  case class UnableToLockErrors(errors: List[UnableToLock]) extends BusinessErrors

  case class UnableToUnlock(transactionId: Long, account: Long, cause: String) extends BusinessError
  case class UnableToUnlockErrors(errors: List[UnableToUnlock]) extends BusinessErrors

  case class UnableToRead(transactionId: Long, account: Long, cause: String) extends BusinessError
  case class UnableToReadErrors(errors: List[UnableToRead]) extends BusinessErrors

  case class BankTransferDisapproved(transactionId: Long, account: Long, bankTransfer: BankTransfer, cause: String) extends BusinessError
  case class BankTransferDisapprovedErrors(errors: List[BankTransferDisapproved]) extends BusinessErrors

  case class AccountInitiationFailed(transactionId: Long, account: Long, accountInitiation: AccountInitiation, cause: String) extends BusinessError
  case class AccountInitiationFailedErrors(errors: List[AccountInitiationFailed]) extends BusinessErrors

  case class BankTransferFailed(transactionId: Long, account: Long, bankTransfer: BankTransfer, cause: String) extends BusinessError
  case class BankTransferFailedErrors(errors: List[BankTransferFailed]) extends BusinessErrors

  case class TransactionReversionFailed(transactionId: Long, account: Long, bankTransfer: BankTransfer, cause: String) extends BusinessError
  case class TransactionReversionFailedErrors(errors: List[TransactionReversionFailed]) extends BusinessErrors

  type Result[E <: BusinessError, A <: BusinessEntity] = Future[Either[List[E], A]]
  type ErrorsOr[A] = EitherT[Future, BusinessErrors, A]

  // Will never been raised
  case class InternalRetryException(errors: Either[BusinessErrors, Locked]) extends RuntimeException

  class Client(cluster: ActorRef, retriesDelay: FiniteDuration, maxRetries: Int, multi: Multi) extends Actor with ActorLogging {
    import cats.implicits._
    import context.dispatcher
    import multi.single._
    import multi._

    override def receive: Receive = {

      case external.InitAccount(accountInitiation) =>
        performAccountInitiation(accountInitiation)

      case external.PerformBankTransfer(bankTransfer) =>
        performBankTransfer(bankTransfer)

      case external.PerformBankTransfer(bankTransfer) =>
        performBankTransfer(bankTransfer)

      case external.RevertBankTransfer(transactionId, bankTransfer) =>
        performReversion(transactionId, bankTransfer)

      case external.GetRecords(accountId) =>
        performRecordsRetrieving(accountId)
    }

    def performAccountInitiation(accountInitiation: AccountInitiation): Unit = {

      val transactionId: Long = generateTransactionId
      val requester = sender()

      (for {
        _       <- retriableLockAccount(cluster, transactionId, accountInitiation.accountId, retriesDelay, maxRetries)
        result  <- applySingleTransaction(cluster, transactionId, accountInitiation)
        _       <- unlockAccount(cluster, transactionId, accountInitiation.accountId)
      } yield result).value map {
        case Right(TransactionApplied) =>
          requester ! external.AccountInitiationComplete(accountInitiation)

        case Left(errors) =>
          errors match {
            case UnableToLockErrors(e) =>
              log error s"Unable to lock account id [${accountInitiation.accountId}] during transaction [$transactionId]. Starting recovery from [${e mkString ""}]..."
              unlockAfterErrors(cluster, transactionId, accountInitiation.accountId, e)
              log error s"Account initiation [$accountInitiation] for id [${accountInitiation.accountId}] failed during transaction [$transactionId]. Cause: [${e mkString ""}]"
              requester ! external.AccountInitiationFailed(accountInitiation, e mkString ", ")

            case UnableToUnlockErrors(e) =>
              log error s"Unable to unlock account id [${accountInitiation.accountId}] during transaction [$transactionId]. Starting recovery from [${e mkString ""}]..."
              unlockAfterErrors(cluster, transactionId, accountInitiation.accountId, e)
              log error s"Account initiation [$accountInitiation] for id [${accountInitiation.accountId}] failed during transaction [$transactionId]. Cause: [${e mkString ""}]"
              requester ! external.AccountInitiationFailed(accountInitiation, e mkString ", ")

            case AccountInitiationFailedErrors(e) =>
              log error s"Unable to lock account id [${accountInitiation.accountId}] during transaction [$transactionId]. Starting recovery from [${e mkString ""}]..."
              unlockAfterErrors(cluster, transactionId, accountInitiation.accountId, e)
              log error s"Account initiation [$accountInitiation] for id [${accountInitiation.accountId}] failed during transaction [$transactionId]. Cause: [${e mkString ""}]"
              requester ! external.AccountInitiationFailed(accountInitiation, e mkString ", ")
          }
      }
    }

    def performBankTransfer(bankTransfer: BankTransfer): Unit = {
      // To avoid deadlocks we must preserve the order of locks
      val (firstLock, secondLock) = lockOrder(bankTransfer)

      val transactionId: Long = generateTransactionId
      val requester = sender()

      (for {
        _       <- retriableLockAccounts(cluster, transactionId, firstLock, secondLock, retriesDelay, maxRetries)
        // There is no `withFilter` method in EitherT, so we can't use pattern matching for a tuple here
        records <- readAccounts(cluster, transactionId, bankTransfer.from, bankTransfer.to)
        _       <- approveBankTransfer(transactionId, records._1, records._2, bankTransfer)
        result  <- applyBankTransfer(cluster, transactionId, bankTransfer)
        _       <- unlockAccounts(cluster, transactionId, bankTransfer.from, bankTransfer.to)

      } yield result).value map {
        case Right(TransactionApplied) =>
          requester ! external.BankTransferComplete(bankTransfer)

        case Left(errors) =>
          errors match {
            case UnableToLockErrors(e) =>
              log error s"Unable to lock account ids [$firstLock], [$secondLock] during transaction [$transactionId]. Starting recovery from [${e mkString ""}]..."
              unlockAfterErrors(cluster, transactionId, bankTransfer, e)
              log error s"Bank transfer [$bankTransfer] failed for account ids [$firstLock], [$secondLock] during transaction [$transactionId]. Cause: [${e mkString ""}]"
              requester ! external.BankTransferFailed(bankTransfer, e mkString ", ")

            case UnableToUnlockErrors(e) =>
              log error s"Unable to unlock account ids [$firstLock], [$secondLock] during transaction [$transactionId]. Starting recovery from [${e mkString ""}]..."
              unlockAfterErrors(cluster, transactionId, bankTransfer, e)
              log error s"Bank transfer [$bankTransfer] failed for account ids [$firstLock], [$secondLock] during transaction [$transactionId]. Cause: [${e mkString ""}]"
              requester ! external.BankTransferFailed(bankTransfer, e mkString ", ")

            case UnableToReadErrors(e) =>
              log error s"Unable to read records for account ids [$firstLock], [$secondLock] during transaction [$transactionId]. Starting recovery from [${e mkString ""}]..."
              unlockAfterErrors(cluster, transactionId, bankTransfer, e)
              log error s"Bank transfer [$bankTransfer] failed for account ids [$firstLock], [$secondLock] during transaction [$transactionId]. Cause: [${e mkString ""}]"
              requester ! external.BankTransferFailed(bankTransfer, e mkString ", ")

            case BankTransferDisapprovedErrors(e) =>
              log error s"Bank transfer was disapproved for account ids [$firstLock], [$secondLock] during transaction [$transactionId]. Starting recovery from [${e mkString ""}]..."
              unlockAfterErrors(cluster, transactionId, bankTransfer, e)
              log error s"Bank transfer [$bankTransfer] failed for account ids [$firstLock], [$secondLock] during transaction [$transactionId]. Cause: [${e mkString ""}]"
              requester ! external.BankTransferFailed(bankTransfer, e mkString ", ")

            case BankTransferFailedErrors(e) =>
              log error s"Failure during transfer for account ids [$firstLock], [$secondLock] during transaction [$transactionId]. Starting recovery from [${e mkString ""}]..."
              revertAfterErrors(cluster, transactionId, bankTransfer, e)
              unlockAfterErrors(cluster, transactionId, bankTransfer, e)
              log error s"Bank transfer [$bankTransfer] failed for account ids [$firstLock], [$secondLock] during transaction [$transactionId]. Cause: [${e mkString ""}]"
              requester ! external.BankTransferFailed(bankTransfer, e mkString ", ")
          }
      }
    }

    def performReversion(transactionId: Long, bankTransfer: BankTransfer): Unit = {
      val (firstLock, secondLock) = lockOrder(bankTransfer)

      val requester = sender()

      (for {
        _       <- retriableLockAccounts(cluster, transactionId, firstLock, secondLock, retriesDelay, maxRetries)
        result  <- revertBankTransfer(cluster, transactionId, bankTransfer)
        _       <- unlockAccounts(cluster, transactionId, bankTransfer.from, bankTransfer.to)
      } yield result).value map {
        case Right(TransactionApplied) =>
          requester ! external.BankTransferReversionComplete(transactionId, bankTransfer)

        case Left(errors) =>
          errors match {
            case UnableToLockErrors(e) =>
              log error s"Unable to lock account ids [$firstLock], [$secondLock] during transaction [$transactionId]. Starting recovery from [${e mkString ""}]..."
              unlockAfterErrors(cluster, transactionId, bankTransfer, e)
              log error s"Bank transfer reversion [$bankTransfer] failed for account ids [$firstLock], [$secondLock] during transaction [$transactionId]. Cause: [${e mkString ""}]"
              requester ! external.BankTransferReversionFailed(transactionId, bankTransfer, e mkString ", ")

            case UnableToUnlockErrors(e) =>
              log error s"Unable to unlock account ids [$firstLock], [$secondLock] during transaction [$transactionId]. Starting recovery from [${e mkString ""}]..."
              unlockAfterErrors(cluster, transactionId, bankTransfer, e)
              log error s"Bank transfer reversion [$bankTransfer] failed for account ids [$firstLock], [$secondLock] during transaction [$transactionId]. Cause: [${e mkString ""}]"
              requester ! external.BankTransferReversionFailed(transactionId, bankTransfer, e mkString ", ")

            case TransactionReversionFailedErrors(e) =>
              log error s"Bank transfer reversion [$bankTransfer] failed for account ids [$firstLock], [$secondLock] during transaction [$transactionId]. Cause: [${e mkString ""}]"
              requester ! external.BankTransferReversionFailed(transactionId, bankTransfer, e mkString ", ")
          }
      }

    }

    def performRecordsRetrieving(accountId: Long): Unit ={
      val requester = sender()
      val transactionId: Long = generateTransactionId

      (for {
        records <- readAccount(cluster, transactionId, accountId)
      } yield records).value map {
        case Right(Records(accountId: Long, records: Seq[Transaction])) =>
          requester ! external.RecordsAndBalance(accountId, single.calculateBalance(accountId, records), records)

        case Left(errors) =>
          errors match {
            case UnableToReadErrors(e) =>
              log error s"Records retrieving failed for account id [$accountId] during transaction [$transactionId]. Cause: [${e mkString ""}]"
              requester ! external.RecordsAndBalanceFailed(accountId, e mkString ", ")
          }
      }
    }

    def lockOrder(bankTransfer: BankTransfer): (Long, Long) =
      if(bankTransfer.from < bankTransfer.to)
        (bankTransfer.from, bankTransfer.to)
      else
        (bankTransfer.to, bankTransfer.from)

    // TODO: Use more "random" generator
    def generateTransactionId: Long =
      Math.abs(Random.nextLong())
  }

  // We have to use `single` and `multi` objects
  // because of cats' imports conflict
  case class Single(implicit timeout: Timeout) {
    import cats.syntax.either._

    import concurrent.ExecutionContext.Implicits.global

    def calculateBalance(accountId: Long, records: Seq[Transaction]): external.Calculations =
      (records groupBy { transaction => transaction.id }).map {
        case (_, transactions) =>
          val reversions = transactions.count(_.`type` == Reverted)

          transactions.find(_.`type` == Committed) map { transfer =>
            if(reversions % 2 == 0)
              transfer.amount
            else
              0L
          }
      }.foldLeft[external.Calculations](external.Balance(accountId, 0)) {
        case (external.Balance(`accountId`, balance), Some(value: Long)) =>
          external.Balance(accountId, balance + value)

        case (corrupted @ external.CorruptedBalance(`accountId`, _), _) =>
          corrupted

        case (_, None) =>
          external.CorruptedBalance(accountId, s"Corrupted balance: there is no transfer transaction for [$accountId]")
      }

    def lock(actor: ActorRef, transactionId: Long, account: Long): Result[UnableToLock, Locked] =
      (actor ? client.Lock(transactionId, account)) map {
        case client.Locked(`transactionId`, `account`) =>
          Locked.asRight

        case error =>
          (UnableToLock(transactionId, account, error.toString) :: Nil).asLeft[Locked]
      } recover {
        case NonFatal(error) =>
          unlock(actor, transactionId, account)
          (UnableToLock(transactionId, account, error.toString) :: Nil).asLeft[Locked]
      }

    def unlock(actor: ActorRef, transactionId: Long, account: Long): Result[UnableToUnlock, Unlocked] =
      (actor ? client.Unlock(transactionId, account)) map {
        case client.Unlocked(`transactionId`, `account`) =>
          Unlocked.asRight

        case error =>
          (UnableToUnlock(transactionId, account, error.toString) :: Nil).asLeft[Unlocked]
      } recover {
        case NonFatal(error) =>
          (UnableToUnlock(transactionId, account, error.toString) :: Nil).asLeft[Unlocked]
      }

    def read(actor: ActorRef, transactionId: Long, accountId: Long): Result[UnableToRead, Records] = {

      (actor ? client.Read(transactionId, accountId)) map {
        case client.Records(`transactionId`, `accountId`, records: Seq[Transaction]) =>
          Records(accountId, records).asRight

        case error =>
          (UnableToRead(transactionId, accountId, error.toString) :: Nil).asLeft[Records]
      } recover {
        case NonFatal(error) =>
          (UnableToRead(transactionId, accountId, error.toString) :: Nil).asLeft[Records]

      }
    }

    // There are could be many different ways to approve transaction:
    // different credit limits, restrictions on amount of money transfer and
    // debit, checking for fraud, abroad transfer limits and arrests.
    // Such strategies should be applied as external "decision makers".
    // Here we just checking for a positive balance
    def approve(transactionId: Long, accountId: Long, records: Records, bankTransfer: BankTransfer): Result[BankTransferDisapproved, BankTransferApproved] = {

      val amount = getAmount(accountId, bankTransfer)

      calculateBalance(accountId, records.records) match {
        case external.Balance(`accountId`, balance) if amount + balance >= 0 =>
          Future.successful(BankTransferApproved.asRight[List[BankTransferDisapproved]])

        case external.Balance(`accountId`, _) =>
          Future.successful(
            (BankTransferDisapproved(transactionId, accountId, bankTransfer, s"Insufficient funds for [$accountId]") :: Nil).asLeft[BankTransferApproved]
          )

        case external.CorruptedBalance(`accountId`, cause) =>
          Future.successful(
            (BankTransferDisapproved(transactionId, accountId, bankTransfer, cause) :: Nil).asLeft[BankTransferApproved]
          )
      }
    }

    def getAmount(accountId: Long, bankTransfer: BankTransfer): Long =
      if (bankTransfer.from == accountId)
        -bankTransfer.amount
      else
        bankTransfer.amount

    // TODO: Merge with next
    def applyTransaction[T <: BusinessError](actor: ActorRef,
                                             transactionId: Long,
                                             accountInitiation: AccountInitiation,
                                             transactionType: TransactionType,
                                             handleError: (Long, AccountInitiation, Any) => List[T]): Result[T, TransactionApplied] = {

      val transaction =
        Transaction(
          transactionId,
          accountInitiation.accountId,
          accountInitiation.amount,
          None,
          accountInitiation.description,
          transactionType
        )

      (actor ? client.Append(transactionId, accountInitiation.accountId, transaction)) map {
        case client.Appended(`transactionId`, _) =>
          TransactionApplied.asRight

        case error =>
          handleError(transactionId, accountInitiation, error).asLeft[TransactionApplied]

      } recover {
        case NonFatal(error) =>
          handleError(transactionId, accountInitiation, error).asLeft[TransactionApplied]
      }
    }

    def applyBankTransferTransaction[T <: BusinessError](actor: ActorRef,
                                                         transactionId: Long,
                                                         accountId: Long,
                                                         bankTransfer: BankTransfer,
                                                         transactionType: TransactionType,
                                                         handleError: (Long, Long, BankTransfer, Any) => List[T] ): Result[T, TransactionApplied] = {

      val amount = getAmount(accountId, bankTransfer)

      val transaction =
        Transaction(
          transactionId,
          bankTransfer.from,
          amount,
          Option(bankTransfer.to),
          bankTransfer.description,
          transactionType
        )

      (actor ? client.Append(transactionId, accountId, transaction)) map {
        case client.Appended(`transactionId`, `accountId`) =>
          TransactionApplied.asRight

        case error =>
          handleError(transactionId, accountId, bankTransfer, error).asLeft[TransactionApplied]

      } recover {
        case NonFatal(error) =>
         handleError(transactionId, accountId, bankTransfer, error).asLeft[TransactionApplied]
      }
    }

  }

  case class Multi(single: Single) {
    import cats.implicits._
    import single._

    import concurrent.ExecutionContext.Implicits.global

    def lockAccounts(actor: ActorRef, transactionId: Long, account1: Long, account2: Long): ErrorsOr[Locked] = {
      EitherT(
        for {
          l1 <- lock(actor, transactionId, account1)
          l2 <- lock(actor, transactionId, account2)
        } yield {
          ((l1.toValidated |@| l2.toValidated) map { (_, _) => Locked } toEither) leftMap {
            UnableToLockErrors
          }
        }
      )
    }

    // TODO: Merge with previous
    def lockAccount(actor: ActorRef, transactionId: Long, account: Long): ErrorsOr[Locked] = {
      EitherT(
        for {
          l <- lock(actor, transactionId, account)
        } yield {
          l leftMap {
            UnableToLockErrors
          }
        }
      )
    }

    def retriableLockAccounts(actor: ActorRef, transactionId: Long, account1: Long, account2: Long, retriesDelay: FiniteDuration, maxRetries: Int): ErrorsOr[Locked] = {
      import monix.execution.Scheduler.Implicits.global

      def retryBackoff(source: Task[Either[BusinessErrors, Locked]], maxRetries: Int): Task[Either[BusinessErrors, Locked]] =
        source.onErrorHandleWith {
          case InternalRetryException(errors) =>
            if (maxRetries > 0)
            // Recursive call, it's OK as Monix is stack-safe
              retryBackoff(source, maxRetries - 1)
                .delayExecution(retriesDelay)
            else
              Task.now(errors)
        }

      val source =
        Task({}) flatMap { _ =>
          Task.fromFuture(lockAccounts(actor, transactionId, account1, account2).value)
        } flatMap {
          case r @ Right(Locked)  => Task.now(r)
          case l @ Left(_)        => Task.raiseError(InternalRetryException(l))
        }

      EitherT(retryBackoff(source, maxRetries) runAsync)
    }

    // TODO: Merge with previous
    def retriableLockAccount(actor: ActorRef, transactionId: Long, account: Long, retriesDelay: FiniteDuration, maxRetries: Int): ErrorsOr[Locked] = {
      import monix.execution.Scheduler.Implicits.global

      def retryBackoff(source: Task[Either[BusinessErrors, Locked]], maxRetries: Int): Task[Either[BusinessErrors, Locked]] =
        source.onErrorHandleWith {
          case InternalRetryException(errors) =>
            if (maxRetries > 0)
            // Recursive call, it's OK as Monix is stack-safe
              retryBackoff(source, maxRetries - 1)
                .delayExecution(retriesDelay)
            else
              Task.now(errors)
        }

      val source =
        Task({}) flatMap { _ =>
          Task.fromFuture(lockAccount(actor, transactionId, account).value)
        } flatMap {
          case r @ Right(Locked)  => Task.now(r)
          case l @ Left(_)        => Task.raiseError(InternalRetryException(l))
        }

      EitherT(retryBackoff(source, maxRetries) runAsync)
    }

    // TODO: Merge with next
    def unlockAccount(actor: ActorRef, transactionId: Long, account: Long): ErrorsOr[Unlocked] =
      EitherT(
        for {
          v1 <- unlock(actor, transactionId, account)
        } yield {
          v1 leftMap { UnableToUnlockErrors }
        }
      )

    def unlockAccounts(actor: ActorRef, transactionId: Long, account1: Long, account2: Long): ErrorsOr[Unlocked] =
      EitherT(
        for {
          v1 <- unlock(actor, transactionId, account1)
          v2 <- unlock(actor, transactionId, account2)
        } yield {
          ((v1.toValidated |@| v2.toValidated) map { (_, _) => Unlocked } toEither) leftMap { UnableToUnlockErrors }
        }
      )

    def readAccount(actor: ActorRef, transactionId: Long, account: Long): ErrorsOr[Records] =
      // It was made intent to preserve style
      // Could be easily rewritten with flatMap
      EitherT(
        for {
          v <- read(actor, transactionId, account)
        } yield {
          v leftMap { UnableToReadErrors }
        }
      )

    def readAccounts(actor: ActorRef, transactionId: Long, account1: Long, account2: Long): ErrorsOr[(Records, Records)] =
      EitherT(
        for {
          v1 <- read(actor, transactionId, account1)
          v2 <- read(actor, transactionId, account2)
        } yield {
          ((v1.toValidated |@| v2.toValidated) map { (r1, r2) => (r1, r2) /*TODO: Rewrite*/} toEither)  leftMap { UnableToReadErrors }
        }
      )

    def approveBankTransfer(transactionId: Long, recordsFrom: Records, recordsTo: Records, bankTransfer: BankTransfer): ErrorsOr[BankTransferApproved] =
      EitherT(
        for {
          v1 <- approve(transactionId, bankTransfer.from, recordsFrom, bankTransfer)
          v2 <- approve(transactionId, bankTransfer.to, recordsTo, bankTransfer)
        } yield {
          ((v1.toValidated |@| v2.toValidated) map { (_, _) => BankTransferApproved } toEither)  leftMap { BankTransferDisapprovedErrors }
        }
      )

    def applyBankTransfer(actor: ActorRef,
                          transactionId: Long,
                          bankTransfer: BankTransfer): ErrorsOr[TransactionApplied] = {

      def errorHandler(transactionId: Long, accountId: Long, bankTransfer: BankTransfer, error: Any): List[BankTransferFailed] =
        BankTransferFailed(transactionId, accountId, bankTransfer, error.toString) :: Nil

      EitherT(
        for {
          v1Committed <- applyBankTransferTransaction(actor, transactionId, bankTransfer.from, bankTransfer, Committed, errorHandler)
          v2Committed <- applyBankTransferTransaction(actor, transactionId, bankTransfer.to, bankTransfer, Committed, errorHandler)
        } yield {
          ((v1Committed.toValidated |@| v2Committed.toValidated) map {
            (_, _) => TransactionApplied
          } toEither) leftMap {
            BankTransferFailedErrors
          }
        }
      )
    }

    // TODO: Merge with previous
    def applySingleTransaction(actor: ActorRef,
                               transactionId: Long,
                               accountInitiation: AccountInitiation): ErrorsOr[TransactionApplied] = {

      def errorHandler(transactionId: Long, accountInitiation: AccountInitiation, error: Any): List[AccountInitiationFailed] =
        AccountInitiationFailed(transactionId, accountInitiation.accountId, accountInitiation, error.toString) :: Nil

      EitherT(
        for {
          vCommitted <- applyTransaction(actor, transactionId, accountInitiation, Committed, errorHandler)
        } yield {
          vCommitted leftMap {
            AccountInitiationFailedErrors
          }
        }
      )
    }

    def revertBankTransfer(actor: ActorRef,
                           transactionId: Long,
                           bankTransfer: BankTransfer): ErrorsOr[TransactionApplied] = {

      def errorHandler(transactionId: Long, accountId: Long, bankTransfer: BankTransfer, error: Any): List[TransactionReversionFailed] =
        TransactionReversionFailed(transactionId, accountId, bankTransfer, error.toString) :: Nil

      EitherT(
        for {
          v1Reverted <- applyBankTransferTransaction(actor, transactionId, bankTransfer.from, bankTransfer, Reverted, errorHandler)
          v2Reverted <- applyBankTransferTransaction(actor, transactionId, bankTransfer.to, bankTransfer, Reverted, errorHandler)
        } yield {
          ((v1Reverted.toValidated |@| v2Reverted.toValidated) map {
            (_, _) => TransactionApplied
          } toEither) leftMap {
            TransactionReversionFailedErrors
          }
        }
      )

    }

    def revertTransaction(actor: ActorRef,
                          transactionId: Long,
                          accountId: Long,
                          bankTransfer: BankTransfer): ErrorsOr[TransactionApplied] = {

      def errorHandler(transactionId: Long, accountId: Long, bankTransfer: BankTransfer, error: Any): List[TransactionReversionFailed] =
        TransactionReversionFailed(transactionId, accountId, bankTransfer, error.toString) :: Nil

      EitherT(
        for {
          vReverted <- applyBankTransferTransaction(actor, transactionId, bankTransfer.from, bankTransfer, Reverted, errorHandler)
        } yield {
          vReverted leftMap {
            TransactionReversionFailedErrors
          }
        }
      )
    }

    def unlockAfterErrors(actor: ActorRef, transactionId: Long, bankTransfer: BankTransfer, errors: List[BusinessError]): Unit =
      (bankTransfer.from :: bankTransfer.to :: Nil) map { account =>
        unlock(actor, transactionId, account) map { result =>
          result map { r =>
            /*TODO: Log*/
          } leftMap { l =>
            /*TODO: Log*/
          }
        } recover {
          case NonFatal(e) =>
          /*TODO: Log*/
        }
      }

    // TODO: Merge with previous
    def unlockAfterErrors(actor: ActorRef, transactionId: Long, accountId: Long, errors: List[BusinessError]): Unit =
      (accountId :: Nil) map { account =>
        unlock(actor, transactionId, account) map { result =>
          result map { r =>
            /*TODO: Log*/
          } leftMap { l =>
            /*TODO: Log*/
          }
        } recover {
          case NonFatal(e) =>
          /*TODO: Log*/
        }
      }

    def revertAfterErrors(actor: ActorRef, transactionId: Long, bankTransfer: BankTransfer, errors: List[BankTransferFailed]): Unit = {
      // Survivorship bias: we need to revert only processed accounts
      val unprocessedAccounts = errors map { _.account }

      (bankTransfer.from :: bankTransfer.to :: Nil) filterNot {
        unprocessedAccounts contains _
      } map { account =>
        revertTransaction(actor, transactionId, account, bankTransfer).value map { result =>
          result map { r =>
            /*TODO: Log*/
          } leftMap { l =>
            /*TODO: Log*/
          }
        } recover {
          case NonFatal(e) =>
          /*TODO: Log*/
        }
      }
    }

  }
}
