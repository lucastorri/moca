package com.github.lucastorri.moca.browser.webkit

import java.io.StringWriter
import java.net.{Proxy, URL, URLConnection, URLStreamHandler, URLStreamHandlerFactory}
import java.nio.CharBuffer
import javafx.application.Platform
import javafx.beans.value.{ChangeListener, ObservableValue}
import javafx.concurrent.Worker.State
import javafx.concurrent.{Worker => JFXWorker}
import javafx.geometry.{HPos, VPos}
import javafx.scene.layout.Region
import javafx.scene.web.WebView
import javax.xml.transform.dom.DOMSource
import javax.xml.transform.stream.StreamResult
import javax.xml.transform.{OutputKeys, TransformerFactory}

import com.github.lucastorri.moca.async.{runnable, spawn}
import com.github.lucastorri.moca.browser.{BrowserSettings, Content, RenderedPage}
import com.github.lucastorri.moca.url.Url
import com.typesafe.scalalogging.StrictLogging

import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.Random

class BrowserWindow private[browser](settings: WebKitSettings) extends Region with StrictLogging {

  val id = Random.alphanumeric.take(32).mkString
  private val browser = new WebView
  private val webEngine = browser.getEngine
  private var current: Url = _
  private var promise: Promise[RenderedPage] = _

  logger.trace(s"Region $id starting")
  getChildren.add(browser)
  webEngine.setUserAgent(settings.base.userAgent)
  webEngine.setJavaScriptEnabled(settings.enableJavaScript)
  webEngine.getLoadWorker.stateProperty().addListener(new ChangeListener[State] {
    override def changed(event: ObservableValue[_ <: State], oldValue: State, newValue: State): Unit = {
      if (event.getValue == JFXWorker.State.SUCCEEDED) {
        promise.success(InternalRenderedPage(current))
      }
    }
  })

  def goTo(url: Url): Future[RenderedPage] = {
    logger.trace(s"Region $id goTo $url")
    val pagePromise = Promise[RenderedPage]()
    Platform.runLater(runnable {
      this.current = url
      this.promise = pagePromise
      webEngine.load(url.toString)
    })
    pagePromise.future
  }

  protected override def layoutChildren(): Unit =
    layoutInArea(browser, 0, 0, getWidth, getHeight, 0, HPos.CENTER, VPos.CENTER)

  protected override def computePrefWidth(height: Double): Double =
    settings.width

  protected override def computePrefHeight(width: Double): Double =
    settings.height

  case class InternalRenderedPage(originalUrl: Url) extends RenderedPage {

    override def currentUrl: Url =
      Url.parse(webEngine.getLocation).getOrElse(originalUrl)

    override def exec(javascript: String): AnyRef = {
      val promise = Promise[AnyRef]()
      Platform.runLater(runnable(promise.success(webEngine.executeScript(javascript))))
      try Await.result(promise.future, settings.loadTimeout)
      catch { case e: Exception => e }
    }

    override def content: Content = {
      val buffer = settings.charset.newEncoder().encode(CharBuffer.wrap(html))
      Content(buffer, "text/html")
    }

    def html: String = {
      val src = new DOMSource(webEngine.getDocument)
      val writer = new StringWriter()
      val transformer = TransformerFactory.newInstance().newTransformer()
      transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8")
      transformer.transform(src, new StreamResult(writer))
      writer.flush()
      writer.toString
    }

    override def settings: BrowserSettings =
      BrowserWindow.this.settings.base

  }

}

object BrowserWindow extends StrictLogging {

  //TODO clear windows that aren't being used for a while
  private val pool = mutable.HashSet.empty[BrowserWindow]
  private val awaiting = mutable.ListBuffer.empty[Promise[BrowserWindow]]
  private val main = Promise[BrowserApplication]()

  URL.setURLStreamHandlerFactory(new MocaURLStreamHandlerFactory)
  Platform.setImplicitExit(false)
  spawn {
    try BrowserLauncher.launch(WebKitBrowserProvider.settings.headless)
    catch { case e: Exception => logger.error("Could not start browser", e) }
  }

  private[browser] def register(view: BrowserApplication): Unit =
    main.success(view)

  private[browser] def get()(implicit exec: ExecutionContext): Future[BrowserWindow] = synchronized {
    val promise = Promise[BrowserWindow]()
    if (pool.isEmpty) {
      main.future.foreach(_.newWindow(WebKitBrowserProvider.settings))
      awaiting += promise
    } else {
      val region = pool.head
      pool.remove(region)
      promise.success(region)
    }
    promise.future
  }

  private[browser] def release(region: BrowserWindow): Unit = synchronized {
    logger.trace(s"Release ${region.id}")
    if (awaiting.nonEmpty) awaiting.remove(0).success(region)
    else pool += region
  }

}

class MocaURLStreamHandlerFactory extends URLStreamHandlerFactory {

  /*TODO
   * cache downloaded content, so if criteria selects an image, etc, they can be downloaded from the cache.
   * Use that to also retrieve the headers of the original request.
   */

  override def createURLStreamHandler(protocol: String): URLStreamHandler = protocol match {
    case "http" => new HttpHandler
    case "https" => new HttpsHandler
    case _ => null
  }

}

class HttpHandler extends sun.net.www.protocol.http.Handler with StrictLogging {

  protected override def openConnection(url: URL): URLConnection = {
    logger.trace(s"Fetching $url")
    super.openConnection(url)
  }

  protected override def openConnection(url: URL, proxy: Proxy): URLConnection = {
    logger.trace(s"Fetching $url")
    super.openConnection(url, proxy)
  }

}

class HttpsHandler extends sun.net.www.protocol.https.Handler with StrictLogging {

  protected override def openConnection(url: URL): URLConnection = {
    logger.trace(s"Fetching $url")
    super.openConnection(url)
  }

  protected override def openConnection(url: URL, proxy: Proxy): URLConnection = {
    logger.trace(s"Fetching $url")
    super.openConnection(url, proxy)
  }

}

  /*
    URL.setURLStreamHandlerFactory(new URLStreamHandlerFactory() {
      def createURLStreamHandler(protocol: String): URLStreamHandler = {
        System.out.println(protocol)
        if (protocol.matches("http")) {
          return new Browser#HttpHandler
        }
        else if (protocol.matches("https")) {
          return new Browser#HttpsHandler
        }
        return null
      }
    })

  private[browser] class HttpHandler extends sun.net.www.protocol.http.Handler {
    @throws(classOf[IOException])
    protected override def openConnection(url: URL): URLConnection = {
      System.out.println(url)
      return a
    }

    @throws(classOf[IOException])
    protected override def openConnection(url: URL, proxy: Proxy): URLConnection = {
      System.out.println(url)
      return super.openConnection(url, proxy)
    }
  }

  private[browser] class HttpsHandler extends sun.net.www.protocol.https.Handler {
    @throws(classOf[IOException])
    protected override def openConnection(url: URL): URLConnection = {
      System.out.println(url)
      return super.openConnection(url)
    }

    @throws(classOf[IOException])
    protected override def openConnection(url: URL, proxy: Proxy): URLConnection = {
      System.out.println(url)
      return super.openConnection(url, proxy)
    }
  }

  def a: URLConnection = {
    val handler: InvocationHandler = new InvocationHandler() {
      @throws(classOf[Throwable])
      def invoke(proxy: AnyRef, method: Method, args: Array[AnyRef]): AnyRef = {
        System.out.println(method)
        return null
      }
    }
    val proxy: URLConnection = java.lang.reflect.Proxy.newProxyInstance(classOf[URLConnection].getClassLoader, Array[Class[_]](classOf[URLConnection]), handler).asInstanceOf[URLConnection]
    return proxy
  }
*/