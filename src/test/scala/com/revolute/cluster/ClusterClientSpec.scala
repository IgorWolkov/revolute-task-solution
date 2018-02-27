package com.revolute.cluster

import akka.actor.{Actor, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.revolute.cluster.ClusterClient._
import com.revolute.cluster.model._
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike, _}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

class ClusterClientSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {

  implicit class CompareHandler[T](future: Future[T]) {
    def `should equal`[V](value: V): Assertion =
      Await.result(future, 20 second) shouldEqual value

    def check(checker: PartialFunction[T, Assertion]): Assertion =
      checker(Await.result(future, 20 second))
  }

  val retriesDelay: FiniteDuration = 1 second
  val maxRetries = 10

  "single" must {
    implicit val timeout: Timeout = Timeout(20 seconds)

    val system = ActorSystem("System")
    val transactionId = 1
    val accountId = 1

    "successfully lock accounts" in {

      class ClusterStub extends Actor {
        override def receive: Receive = {
          case client.Lock(t, k) => sender() ! client.Locked(t, k)
        }
      }

      val cluster = system.actorOf(Props(new ClusterStub))

      ClusterClient.Single().lock(cluster, transactionId, accountId) `should equal`
        Right(ClusterClient.Locked)
    }

    "handle lock failures" in {

      class ClusterStub extends Actor {
        override def receive: Receive = {
          case client.Lock(t, k) => sender() ! client.ExternallyLocked(t, k)
        }
      }

      val cluster = system.actorOf(Props(new ClusterStub))

      ClusterClient.Single().lock(cluster, transactionId, accountId) `should equal`
        Left(ClusterClient.UnableToLock(transactionId, accountId, "ExternallyLocked(1,1)") :: Nil)
    }

    "successfully unlock accounts" in {

      class ClusterStub extends Actor {
        override def receive: Receive = {
          case client.Unlock(t, k) => sender() ! client.Unlocked(t, k)
        }
      }

      val cluster = system.actorOf(Props(new ClusterStub))

      ClusterClient.Single().unlock(cluster, transactionId, accountId) `should equal`
        Right(ClusterClient.Unlocked)
    }

    "handle unlock failures" in {

      class ClusterStub extends Actor {
        override def receive: Receive = {
          case client.Unlock(t, k) => sender() ! client.ExternallyLocked(t, k)
        }
      }

      val cluster = system.actorOf(Props(new ClusterStub))

      ClusterClient.Single().unlock(cluster, transactionId, accountId) `should equal`
        Left(ClusterClient.UnableToUnlock(transactionId, accountId, "ExternallyLocked(1,1)") :: Nil)
    }

    "successfully read records" in {

      class ClusterStub extends Actor {
        override def receive: Receive = {
          case client.Read(t, k) => sender() ! client.Records(t, k, Seq.empty)
        }
      }

      val cluster = system.actorOf(Props(new ClusterStub))

      ClusterClient.Single().read(cluster, transactionId, accountId) `should equal`
        Right(ClusterClient.Records(accountId, Seq.empty))
    }

    "handle failures while reading" in {

      class ClusterStub extends Actor {
        override def receive: Receive = {
          case client.Read(t, k) => sender() ! client.ExternallyLocked(t, k)
        }
      }

      val cluster = system.actorOf(Props(new ClusterStub))

      ClusterClient.Single().read(cluster, transactionId, accountId) `should equal`
        Left(ClusterClient.UnableToRead(transactionId, accountId, "ExternallyLocked(1,1)") :: Nil)
    }

    "calculate balance" in {

      ClusterClient.Single().calculateBalance(1, Seq.empty) shouldBe external.Balance(1, 0)

      ClusterClient.Single().calculateBalance(
        1,
        Seq(Transaction(7, 1, 42, None, "", Committed))
      ) shouldBe external.Balance(1, 42)

      ClusterClient.Single().calculateBalance(
        1,
        Seq(
          Transaction(7, 1, 42, None, "", Committed),
          Transaction(7, 1, 42, None, "", Reverted)
        )
      ) shouldBe external.Balance(1, 0)

      ClusterClient.Single().calculateBalance(
        1,
        Seq(
          Transaction(7, 1, 42, None, "", Committed),
          Transaction(7, 1, 42, None, "", Reverted),
          Transaction(7, 1, 42, None, "", Reverted)
        )
      ) shouldBe external.Balance(1, 42)

      ClusterClient.Single().calculateBalance(
        1,
        Seq(
          Transaction(7, 1, 42, None, "", Committed),
          Transaction(7, 1, 42, None, "", Reverted),
          Transaction(7, 1, 42, None, "", Reverted),
          Transaction(11, 1, 42, None, "", Committed),
          Transaction(17, 1, 42, None, "", Committed),
          Transaction(7, 1, 42, None, "", Reverted)
        )
      ) shouldBe external.Balance(1, 84)

      ClusterClient.Single().calculateBalance(
        1,
        Seq(
          Transaction(7, 1, 42, None, "", Reverted)
        )
      ) shouldBe external.CorruptedBalance(1, s"Corrupted balance: there is no transfer transaction for [1]")

    }

    "successfully approve bank transfer" in {
      val transactionId = 11
      val from = 1
      val to = 2
      val records1 =
        ClusterClient.Records(
          from,
          Seq(
            Transaction(7, from, 42, None, "", Committed),
            Transaction(7, from, 42, None, "", Reverted),
            Transaction(7, from, 42, None, "", Reverted)
          )
        )

      val records2 =
        ClusterClient.Records(
          from,
          Seq(
            Transaction(7, to, 42, None, "", Committed),
            Transaction(7, to, 42, None, "", Reverted),
            Transaction(7, to, 42, None, "", Reverted)
          )
        )

      ClusterClient.Single().approve(transactionId, from, records1, BankTransfer(from, to, 42, "BankTransfer")) `should equal`
        Right(BankTransferApproved)

      ClusterClient.Single().approve(transactionId, from, records1, BankTransfer(from, to, 41, "BankTransfer"))  `should equal`
        Right(BankTransferApproved)

      ClusterClient.Single().approve(transactionId, from, records1, BankTransfer(from, to, -100, "BankTransfer"))  `should equal`
        Right(BankTransferApproved)

      ClusterClient.Single().approve(transactionId, to, records2, BankTransfer(from, to, 42, "BankTransfer")) `should equal`
        Right(BankTransferApproved)

      ClusterClient.Single().approve(transactionId, to, records2, BankTransfer(from, to, 41, "BankTransfer"))  `should equal`
        Right(BankTransferApproved)

      ClusterClient.Single().approve(transactionId, to, records2, BankTransfer(from, to, 100, "BankTransfer"))  `should equal`
        Right(BankTransferApproved)

    }

    "handle approve failures" in {

      val transactionId = 11
      val from = 1
      val to = 2
      val records1 =
        ClusterClient.Records(
          from,
          Seq(
            Transaction(7, from, 42, None, "", Committed),
            Transaction(7, from, 42, None, "", Reverted),
            Transaction(7, from, 42, None, "", Reverted)
          )
        )

      val records2 =
        ClusterClient.Records(
          to,
          Seq(
            Transaction(7, to, 42, None, "", Committed),
            Transaction(7, to, 42, None, "", Reverted),
            Transaction(7, to, 42, None, "", Reverted)
          )
        )

      val corruptedRecords =
        ClusterClient.Records(
          to,
          Seq(
            Transaction(7, to, 42, None, "", Reverted)
          )
        )

      ClusterClient.Single().approve(transactionId, from, records1, BankTransfer(from, to, 43, "BankTransfer")) `should equal`
        Left(BankTransferDisapproved(transactionId, from, BankTransfer(from, to, 43, "BankTransfer"), s"Insufficient funds for [$from]") :: Nil)

      ClusterClient.Single().approve(transactionId, to, records2, BankTransfer(from, to, -43, "BankTransfer")) `should equal`
        Left(BankTransferDisapproved(transactionId, to, BankTransfer(from, to, -43, "BankTransfer"), s"Insufficient funds for [$to]") :: Nil)

      ClusterClient.Single().approve(transactionId, to, corruptedRecords, BankTransfer(from, to, 10, "BankTransfer")) `should equal`
        Left(BankTransferDisapproved(transactionId, to, BankTransfer(from, to, 10, "BankTransfer"), s"Corrupted balance: there is no transfer transaction for [$to]") :: Nil)

    }

    "successfully apply transaction for account" in {

      class ClusterStub extends Actor {
        override def receive: Receive = {
          case client.Append(t, k, _) => sender() ! client.Appended(t, k)
        }
      }

      val cluster = system.actorOf(Props(new ClusterStub))

      val bankTransfer = BankTransfer(from = accountId, to = 2, amount = 42, description = "BankTransfer")

      val handleError: (Long, Long, BankTransfer, Any) => List[BankTransferFailed] =
        (transactionId, accountId, bankTransfer, error) =>
          BankTransferFailed(transactionId, accountId, bankTransfer, error.toString) :: Nil

      ClusterClient.Single().applyBankTransferTransaction(cluster, transactionId, accountId, bankTransfer, New, handleError) `should equal`
        Right(ClusterClient.TransactionApplied)
    }

    "handle transaction application failures" in {

      class ClusterStub extends Actor {
        override def receive: Receive = {
          case client.Append(t, k, _) => sender() ! client.ExternallyLocked(t, k)
        }
      }

      val cluster = system.actorOf(Props(new ClusterStub))

      val bankTransfer = BankTransfer(from = accountId, to = 2, amount = 42, description = "BankTransfer")

      val handleError: (Long, Long, BankTransfer, Any) => List[BankTransferFailed] =
        (transactionId, accountId, bankTransfer, error) =>
          BankTransferFailed(transactionId, accountId, bankTransfer, error.toString) :: Nil

      ClusterClient.Single().applyBankTransferTransaction(cluster, transactionId, accountId, bankTransfer, New, handleError) `should equal`
        Left(ClusterClient.BankTransferFailed(transactionId, accountId, bankTransfer, "ExternallyLocked(1,1)") :: Nil)

    }

  }

  "multi" must {
    implicit val timeout: Timeout = Timeout(20 seconds)

    val system = ActorSystem("System")
    val transactionId = 1
    val from = 1
    val to = 2

    "lock two client account ids and/or handle errors" in {

      class ClusterStub extends Actor {
        var index = 0
        val responses: List[(Long, Long) => client.ClientMessage with Product with Serializable] =
          ((t: Long, k: Long) => client.Locked(t, k)) ::
          ((t: Long, k: Long) => client.Locked(t, k)) ::
          ((t: Long, k: Long) => client.Locked(t, k)) ::
          ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
          ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
          ((t: Long, k: Long) => client.ExternallyLocked(t, k)) :: Nil

        override def receive: Receive = {
          case client.Lock(t, k) => sender() ! responses(index)(t, k); index += 1
        }
      }

      val cluster = system.actorOf(Props(new ClusterStub))

      ClusterClient.Multi(ClusterClient.Single()).lockAccounts(cluster, transactionId, from, to).value `should equal`
        Right(ClusterClient.Locked)

      ClusterClient.Multi(ClusterClient.Single()).lockAccounts(cluster, transactionId, from, to).value `should equal`
        Left(UnableToLockErrors(
          ClusterClient.UnableToLock(transactionId, to, "ExternallyLocked(1,2)") :: Nil
        ))

      ClusterClient.Multi(ClusterClient.Single()).lockAccounts(cluster, transactionId, from, to).value `should equal`
        Left(UnableToLockErrors(
          ClusterClient.UnableToLock(transactionId, from, "ExternallyLocked(1,1)") ::
          ClusterClient.UnableToLock(transactionId, to, "ExternallyLocked(1,2)") :: Nil
        ))
    }

    "lock with reties two account ids and/or handle errors" in {

      class ClusterStub extends Actor {
        var index = 0
        val responses: List[(Long, Long) => client.ClientMessage with Product with Serializable] =
          // Successful flow
          ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            // Error flow
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) :: Nil


        override def receive: Receive = {
          case client.Lock(t, k) =>
            sender() ! responses(index)(t, k); index += 1
        }
      }

      val cluster = system.actorOf(Props(new ClusterStub))

      ClusterClient.Multi(ClusterClient.Single()).retriableLockAccounts(cluster, transactionId, from, to, retriesDelay, maxRetries).value `should equal`
        Right(ClusterClient.Locked)

      ClusterClient.Multi(ClusterClient.Single()).lockAccounts(cluster, transactionId, from, to).value `should equal`
        Left(UnableToLockErrors(
          ClusterClient  .UnableToLock(transactionId, from, "ExternallyLocked(1,1)") ::
            ClusterClient  .UnableToLock(transactionId, to, "ExternallyLocked(1,2)") :: Nil
        ))
    }

    "unlock two account ids and/or handle errors" in {

      class ClusterStub extends Actor {
        var index = 0
        val responses: List[(Long, Long) => client.ClientMessage with Product with Serializable] =
          ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) :: Nil

        override def receive: Receive = {
          case client.Unlock(t, k) => sender() ! responses(index)(t, k); index += 1
        }
      }

      val cluster = system.actorOf(Props(new ClusterStub))

      ClusterClient.Multi(ClusterClient.Single()).unlockAccounts(cluster, transactionId, from, to).value `should equal`
        Right(ClusterClient.Unlocked)

      ClusterClient.Multi(ClusterClient.Single()).unlockAccounts(cluster, transactionId, from, to).value `should equal`
        Left(UnableToUnlockErrors(
          ClusterClient.UnableToUnlock(transactionId, to, "ExternallyLocked(1,2)") :: Nil
        ))

      ClusterClient.Multi(ClusterClient.Single()).unlockAccounts(cluster, transactionId, from, to).value `should equal`
        Left(UnableToUnlockErrors(
          ClusterClient.UnableToUnlock(transactionId, from, "ExternallyLocked(1,1)") ::
          ClusterClient.UnableToUnlock(transactionId, to, "ExternallyLocked(1,2)") :: Nil
        ))
    }

    "read records for single account id and/or handle errors" in {

      class ClusterStub extends Actor {
        var index = 0
        val responses: List[(Long, Long) => client.ClientMessage with Product with Serializable] =
          ((t: Long, k: Long) => client.Records(t, k, Seq.empty)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) :: Nil

        override def receive: Receive = {
          case client.Read(t, k) => sender() ! responses(index)(t, k); index += 1
        }
      }

      val cluster = system.actorOf(Props(new ClusterStub))

      ClusterClient.Multi(ClusterClient.Single()).readAccount(cluster, transactionId, from).value `should equal`
        Right(ClusterClient.Records(from, Seq.empty))

      ClusterClient.Multi(ClusterClient.Single()).readAccount(cluster, transactionId, from).value `should equal`
        Left(UnableToReadErrors(
          ClusterClient.UnableToRead(transactionId, from, "ExternallyLocked(1,1)") :: Nil
        ))

    }

    "read records for two account ids and/or handle errors" in {

      class ClusterStub extends Actor {
        var index = 0
        val responses: List[(Long, Long) => client.ClientMessage with Product with Serializable] =
          ((t: Long, k: Long) => client.Records(t, k, Seq.empty)) ::
            ((t: Long, k: Long) => client.Records(t, k, Seq.empty)) ::
            ((t: Long, k: Long) => client.Records(t, k, Seq.empty)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) :: Nil

        override def receive: Receive = {
          case client.Read(t, k) => sender() ! responses(index)(t, k); index += 1
        }
      }

      val cluster = system.actorOf(Props(new ClusterStub))

      ClusterClient.Multi(ClusterClient.Single()).readAccounts(cluster, transactionId, from, to).value `should equal`
        Right((ClusterClient.Records(from, Seq.empty), ClusterClient.Records(to, Seq.empty)))

      ClusterClient.Multi(ClusterClient.Single()).readAccounts(cluster, transactionId, from, to).value `should equal`
        Left(UnableToReadErrors(
          ClusterClient.UnableToRead(transactionId, to, "ExternallyLocked(1,2)") :: Nil
        ))

      ClusterClient.Multi(ClusterClient.Single()).readAccounts(cluster, transactionId, from, to).value `should equal`
        Left(UnableToReadErrors(
          ClusterClient.UnableToRead(transactionId, from, "ExternallyLocked(1,1)") ::
            ClusterClient.UnableToRead(transactionId, to, "ExternallyLocked(1,2)") :: Nil
        ))
    }

    "apply bank transfer for two account ids and/or handle errors" in {

      class ClusterStub extends Actor {
        var index = 0
        val responses: List[(Long, Long) => client.ClientMessage with Product with Serializable] =
            ((t: Long, k: Long) => client.Appended(t, k)) ::
            ((t: Long, k: Long) => client.Appended(t, k)) ::
            ((t: Long, k: Long) => client.Appended(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) :: Nil

        override def receive: Receive = {
          case client.Append(t, k, _) => sender() ! responses(index)(t, k); index += 1
        }
      }

      val cluster = system.actorOf(Props(new ClusterStub))

      val bankTransfer = BankTransfer(from, to, amount = 42, description = "BankTransfer")

      ClusterClient.Multi(ClusterClient.Single()).applyBankTransfer(cluster, transactionId, bankTransfer).value `should equal`
        Right(ClusterClient.TransactionApplied)

      ClusterClient.Multi(ClusterClient.Single()).applyBankTransfer(cluster, transactionId, bankTransfer).value `should equal`
        Left(BankTransferFailedErrors(
          ClusterClient.BankTransferFailed(transactionId, to, bankTransfer, "ExternallyLocked(1,2)") :: Nil
        ))

      ClusterClient.Multi(ClusterClient.Single()).applyBankTransfer(cluster, transactionId, bankTransfer).value `should equal`
        Left(BankTransferFailedErrors(
          ClusterClient.BankTransferFailed(transactionId, from, bankTransfer, "ExternallyLocked(1,1)") ::
          ClusterClient.BankTransferFailed(transactionId, to, bankTransfer, "ExternallyLocked(1,2)") :: Nil
        ))
    }

    "revert single transaction for single account id and/or handle errors" in {

      class ClusterStub extends Actor {
        var index = 0
        val responses: List[(Long, Long) => client.ClientMessage with Product with Serializable] =
          ((t: Long, k: Long) => client.Appended(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) :: Nil

        override def receive: Receive = {
          case client.Append(t, k, _) => sender() ! responses(index)(t, k); index += 1
        }
      }

      val cluster = system.actorOf(Props(new ClusterStub))

      val bankTransfer = BankTransfer(from, to, amount = 42, description = "BankTransfer")

      ClusterClient.Multi(ClusterClient.Single()).revertTransaction(cluster, transactionId, from, bankTransfer).value `should equal`
        Right(ClusterClient.TransactionApplied)

      ClusterClient.Multi(ClusterClient.Single()).revertTransaction(cluster, transactionId, from, bankTransfer).value `should equal`
        Left(TransactionReversionFailedErrors(
          ClusterClient.TransactionReversionFailed(transactionId, from, bankTransfer, "ExternallyLocked(1,1)") :: Nil
        ))
    }

    "revert bank transfer for two account ids and/or handle errors" in {

      class ClusterStub extends Actor {
        var index = 0
        val responses: List[(Long, Long) => client.ClientMessage with Product with Serializable] =
          ((t: Long, k: Long) => client.Appended(t, k)) ::
          ((t: Long, k: Long) => client.Appended(t, k)) ::
          ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
          ((t: Long, k: Long) => client.Appended(t, k)) :: Nil

        override def receive: Receive = {
          case client.Append(t, k, _) => sender() ! responses(index)(t, k); index += 1
        }
      }

      val cluster = system.actorOf(Props(new ClusterStub))

      val bankTransfer = BankTransfer(from, to, amount = 42, description = "BankTransfer")

      ClusterClient.Multi(ClusterClient.Single()).revertBankTransfer(cluster, transactionId, bankTransfer).value `should equal`
        Right(ClusterClient.TransactionApplied)

      ClusterClient.Multi(ClusterClient.Single()).revertBankTransfer(cluster, transactionId, bankTransfer).value `should equal`
        Left(TransactionReversionFailedErrors(
          ClusterClient.TransactionReversionFailed(transactionId, from, bankTransfer, "ExternallyLocked(1,1)") :: Nil
        ))
    }
  }

  "ClusterClient" must {
    implicit val timeout: Timeout = Timeout(20 seconds)

    val system = ActorSystem("System")
    val to = 2

    "process account initiation and/or handle errors" in {

      class ClusterStub extends Actor {
        var index = 0
        val responses: List[(Long, Long) => client.ClientMessage with Product with Serializable] =
        // Positive flow
          ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Appended(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            // Appending error
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            // 4 attempts for locking
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Appended(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) :: Nil


        override def receive: Receive = {
          case message: client.ClientMessage =>
            sender() ! responses(index)(message.transactionId, message.key); index += 1
        }
      }

      val clusterActor = system.actorOf(Props(new ClusterStub))
      val clientActor = system.actorOf(Props(new Client(clusterActor, retriesDelay, maxRetries, ClusterClient.Multi(ClusterClient.Single()))))

      // Positive flow
      clientActor ? external.InitAccount(AccountInitiation(2, 0, "InitAccount")) `should equal`
        external.AccountInitiationComplete(AccountInitiation(2, 0, "InitAccount"))

      // Appending error
      (clientActor ? external.InitAccount(AccountInitiation(6, 0, "InitAccount"))) check {
        case external.AccountInitiationFailed(AccountInitiation(6, 0, "InitAccount"), _) => succeed
        case _                                                                            => fail()
      }

      // 4 attempts for locking
      (clientActor ? external.InitAccount(AccountInitiation(11, 0, "InitAccount"))) `should equal`
        external.AccountInitiationComplete(AccountInitiation(11, 0, "InitAccount"))
    }

    "process bank transfer and/or handle errors" in {

      class ClusterStub extends Actor {
        var index = 0
        val responses: List[(Long, Long) => client.ClientMessage with Product with Serializable] =
          // Positive flow
          ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Records(t, k, Seq.empty)) ::
            ((t: Long, k: Long) => client.Records(t, k, Seq.empty)) ::
            ((t: Long, k: Long) => client.Appended(t, k)) ::
            ((t: Long, k: Long) => client.Appended(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            // Reading error
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Records(t, k, Seq.empty)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            // Disapproved
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Records(t, k, Seq.empty)) ::
            ((t: Long, k: Long) => client.Records(t, k, Seq.empty)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            // One appending error with one revert: see survivorship bias
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Records(t, k, Seq.empty)) ::
            ((t: Long, k: Long) => client.Records(t, k, Seq.empty)) ::
            ((t: Long, k: Long) => client.Appended(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.Appended(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            // Two appending errors with no reverts
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Records(t, k, Seq.empty)) ::
            ((t: Long, k: Long) => client.Records(t, k, Seq.empty)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            // 4 attempts for locking
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Records(t, k, Seq.empty)) ::
            ((t: Long, k: Long) => client.Records(t, k, Seq.empty)) ::
            ((t: Long, k: Long) => client.Appended(t, k)) ::
            ((t: Long, k: Long) => client.Appended(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) :: Nil


        override def receive: Receive = {
          case message: client.ClientMessage =>
            sender() ! responses(index)(message.transactionId, message.key); index += 1
        }
      }

      val clusterActor = system.actorOf(Props(new ClusterStub))
      val clientActor = system.actorOf(Props(new Client(clusterActor, retriesDelay, maxRetries, ClusterClient.Multi(ClusterClient.Single()))))

      // Positive flow
      clientActor ? external.PerformBankTransfer(bankTransfer = BankTransfer(1, 2, 0, "BankTransfer")) `should equal`
        external.BankTransferComplete(BankTransfer(1, 2, 0, "BankTransfer"))

      // Reading error
      (clientActor ? external.PerformBankTransfer(bankTransfer = BankTransfer(3, 4, 0, "BankTransfer"))) check {
        case external.BankTransferFailed(BankTransfer(3, 4, 0, "BankTransfer"), _) => succeed
        case _                                                                     => fail()
      }

      // Disapproved
      (clientActor ? external.PerformBankTransfer(bankTransfer = BankTransfer(3, 4, 10, "BankTransfer"))) check {
        case external.BankTransferFailed(BankTransfer(3, 4, 10, "BankTransfer"), _) => succeed
        case _                                                                      => fail()
      }

      // One appending error with one revert
      (clientActor ? external.PerformBankTransfer(bankTransfer = BankTransfer(5, 6, 0, "BankTransfer"))) check {
        case external.BankTransferFailed(BankTransfer(5, 6, 0, "BankTransfer"), _) => succeed
        case _                                                                     => fail()
      }

      // Two appending errors with no reverts
      (clientActor ? external.PerformBankTransfer(bankTransfer = BankTransfer(8, 9, 0, "BankTransfer"))) check {
        case external.BankTransferFailed(BankTransfer(8, 9, 0, "BankTransfer"), _) => succeed
        case _                                                                     => fail()
      }

      // 4 attempts for locking
      (clientActor ? external.PerformBankTransfer(bankTransfer = BankTransfer(10, 11, 0, "BankTransfer"))) `should equal`
        external.BankTransferComplete(BankTransfer(10, 11, 0, "BankTransfer"))

    }

    "revert bank transfer and/or handle errors" in {

      class ClusterStub extends Actor {
        var index = 0
        val responses: List[(Long, Long) => client.ClientMessage with Product with Serializable] =
        // Positive flow
          ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Appended(t, k)) ::
            ((t: Long, k: Long) => client.Appended(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            // One appending error
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Appended(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            // Two appending errors
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            // 4 attempts for locking
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.ExternallyLocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Locked(t, k)) ::
            ((t: Long, k: Long) => client.Appended(t, k)) ::
            ((t: Long, k: Long) => client.Appended(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) ::
            ((t: Long, k: Long) => client.Unlocked(t, k)) :: Nil


        override def receive: Receive = {
          case message: client.ClientMessage =>
            sender() ! responses(index)(message.transactionId, message.key); index += 1
        }
      }

      val clusterActor = system.actorOf(Props(new ClusterStub))
      val clientActor = system.actorOf(Props(new Client(clusterActor, retriesDelay, maxRetries, ClusterClient.Multi(ClusterClient.Single()))))

      // Positive flow
      clientActor ? external.RevertBankTransfer(1, BankTransfer(1, 2, 10, "BankTransfer")) `should equal`
        external.BankTransferReversionComplete(1, BankTransfer(1, 2, 10, "BankTransfer"))

      // One appending error
      (clientActor ? external.RevertBankTransfer(1, BankTransfer(5, 6, 10, "BankTransfer"))) check {
        case external.BankTransferReversionFailed(1, BankTransfer(5, 6, 10, "BankTransfer"), _) => succeed
        case _                                                                                  => fail()
      }

      // Two appending errors
      (clientActor ? external.RevertBankTransfer(1, BankTransfer(8, 9, 10, "BankTransfer"))) check {
        case external.BankTransferReversionFailed(1, BankTransfer(8, 9, 10, "BankTransfer"), _) => succeed
        case _                                                                                  => fail()
      }

      // 4 attempts for locking
      (clientActor ? external.RevertBankTransfer(1, BankTransfer(10, 11, 10, "BankTransfer"))) `should equal`
        external.BankTransferReversionComplete(1, BankTransfer(10, 11, 10, "BankTransfer"))

    }

    "retrieve records and/or handle errors" in {

      class ClusterStub extends Actor {
        var index = 0
        val responses: List[(Long, Long) => client.ClientMessage with Product with Serializable] =
        // Positive flow
          ((t: Long, k: Long) => client.Records(t, k, Seq.empty)) ::
          ((t: Long, k: Long) => client.Records(t, k, Seq(Transaction(7, to, 42, None, "", Committed)))) ::
          ((t: Long, k: Long) => client.Records(t, k, Seq(Transaction(7, to, 42, None, "", Reverted)))) :: Nil


        override def receive: Receive = {
          case message: client.ClientMessage =>
            sender() ! responses(index)(message.transactionId, message.key); index += 1
        }
      }

      val clusterActor = system.actorOf(Props(new ClusterStub))
      val clientActor = system.actorOf(Props(new Client(clusterActor, retriesDelay, maxRetries, ClusterClient.Multi(ClusterClient.Single()))))
      val accountId = 1

      // Positive flow
      clientActor ? external.GetRecords(accountId) `should equal`
        external.RecordsAndBalance(accountId, external.Balance(accountId, 0), Seq.empty)

      clientActor ? external.GetRecords(accountId) `should equal`
        external.RecordsAndBalance(accountId, external.Balance(accountId, 42), Seq(Transaction(7, to, 42, None, "", Committed)))

      // Corrupted records
      clientActor ? external.GetRecords(accountId) `should equal`
        external.RecordsAndBalance(accountId, external.CorruptedBalance(accountId, s"Corrupted balance: there is no transfer transaction for [$accountId]"), Seq(Transaction(7, to, 42, None, "", Reverted)))
    }
  }

}
