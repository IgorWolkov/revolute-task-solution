package com.revolute.web.utils

import java.io.{IOException, InputStream}
import java.security.{KeyStore, KeyStoreException, NoSuchAlgorithmException, SecureRandom}
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}

import akka.http.scaladsl.{ConnectionContext, HttpsConnectionContext}
import cats.syntax.either._

import com.revolute.web.model._

/**
  * Accumulating and validating
  * all configurations for https context
  */
object HttpsServiceContext {

  val KeystoreFileName = "keystoreFileName"
  val KeystorePassword = "keystorePassword"

  val KeystoreType          = "PKCS12"
  val KeyManagerFactoryType = "SunX509"
  val SSLContextType        = "TLS"

  def keyStoreInstance(keyStoreType: String): ErrorsOr[KeyStore] =
    Either.catchOnly[KeyStoreException](
      KeyStore.getInstance(keyStoreType)
    ).leftMap(e => e.getMessage :: Nil)

  def keyManagerFactoryInstance(keyManagerFactoryType: String): ErrorsOr[KeyManagerFactory] =
    Either.catchOnly[NoSuchAlgorithmException](
      KeyManagerFactory.getInstance(keyManagerFactoryType)
    ).leftMap(e => e.getMessage :: Nil)

  def trustManagerFactoryInstance(keyManagerFactoryType: String): ErrorsOr[TrustManagerFactory] =
    Either.catchOnly[NoSuchAlgorithmException](
      TrustManagerFactory.getInstance(keyManagerFactoryType)
    ).leftMap(e => e.getMessage :: Nil)

  def sslContextInstance(sslContextType: String): ErrorsOr[SSLContext] =
    Either.catchOnly[NoSuchAlgorithmException](
      SSLContext.getInstance(sslContextType)
    ).leftMap(e => e.getMessage :: Nil)

  def keystoreFileName(keyStoreFileName: String): ErrorsOr[String] =
    Option(System.getProperty(keyStoreFileName))
      .toRight(s"Keystore file name [-D$keyStoreFileName] property is not specified" :: Nil)

  def keystorePassword(keyStorePassword: String): ErrorsOr[String] =
    Option(System.getProperty(keyStorePassword))
      .toRight(s"Keystore password [-D$keyStorePassword] property is not specified" :: Nil)

  def keyStoreStream(keyStoreFileName: String): ErrorsOr[InputStream] =
    Either.catchOnly[IOException](
      getClass.getClassLoader.getResourceAsStream(keyStoreFileName)
    ).leftMap(_ => s"Cannot load [$keyStoreFileName] keystore file." :: Nil)
      .ensure(s"Cannot load [$keyStoreFileName]" :: Nil)(_ != null)

  def keyStoreStreamInstance: ErrorsOr[InputStream] =
    keystoreFileName(KeystoreFileName) flatMap { fileName =>  keyStoreStream(fileName) }

  def getInstances: ErrorsOr[(KeyStore, KeyManagerFactory, TrustManagerFactory, SSLContext, InputStream, String)] = {
    import cats.implicits._
    (keyStoreInstance(KeystoreType).toValidated |@|
      keyManagerFactoryInstance(KeyManagerFactoryType).toValidated |@|
      trustManagerFactoryInstance(KeyManagerFactoryType).toValidated |@|
      sslContextInstance(SSLContextType).toValidated |@|
      keyStoreStreamInstance.toValidated |@|
      keystorePassword(KeystorePassword).toValidated
      ).map((ks, km, tm, sc, is, ps) => (ks, km, tm, sc, is, ps)).toEither
  }

  def initContext(): ErrorsOr[HttpsConnectionContext] =
    getInstances flatMap {
      case (keyStore, keyManagerFactory, trustManagerFactory, sslContext, inputStream, password) =>
        Either.catchOnly[Exception] {
          keyStore.load(inputStream, password.toCharArray)
          keyManagerFactory.init(keyStore, password.toCharArray)
          trustManagerFactory.init(keyStore)
          sslContext.init(keyManagerFactory.getKeyManagers, trustManagerFactory.getTrustManagers, new SecureRandom)

          ConnectionContext.https(sslContext)
        }.leftMap(e => e.getMessage :: Nil)
    }

}
