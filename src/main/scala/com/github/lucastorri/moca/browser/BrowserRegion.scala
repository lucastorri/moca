package com.github.lucastorri.moca.browser

import java.io.StringWriter
import javafx.application.Platform
import javafx.beans.value.{ChangeListener, ObservableValue}
import javafx.concurrent.Worker.State
import javafx.concurrent.{Worker => JFXWorker}
import javafx.geometry.{HPos, VPos}
import javafx.scene.layout.Region
import javafx.scene.web.WebView
import javax.xml.transform.{OutputKeys, TransformerFactory}
import javax.xml.transform.dom.DOMSource
import javax.xml.transform.stream.StreamResult

import com.github.lucastorri.moca.async.{runnable, spawn}
import com.github.lucastorri.moca.url.Url
import org.jsoup.Jsoup

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Random

class BrowserRegion private[browser](val id: String) extends Region {

  private val browser = new WebView
  private val webEngine = browser.getEngine
  private var current: Url = _
  private var promise: Promise[RenderedPage] = _

  val settings = BrowserRegion.settings(this)

  webEngine.setJavaScriptEnabled(settings.enableJavaScript)
  getChildren.add(browser)

  webEngine.getLoadWorker.stateProperty().addListener(new ChangeListener[State] {
    override def changed(event: ObservableValue[_ <: State], oldValue: State, newValue: State): Unit = {
      if (event.getValue == JFXWorker.State.SUCCEEDED) {
        promise.success(InternalRenderedPage(current))
      }
    }
  })

  def goTo(url: Url): Future[RenderedPage] = {
    this.current = url
    this.promise = Promise[RenderedPage]()
    Platform.runLater(runnable(webEngine.load(url.url)))
    promise.future
  }

  protected override def layoutChildren(): Unit =
    layoutInArea(browser, 0, 0, getWidth, getHeight, 0, HPos.CENTER, VPos.CENTER)

  protected override def computePrefWidth(height: Double): Double =
    settings.width

  protected override def computePrefHeight(width: Double): Double =
    settings.height

  case class InternalRenderedPage(url: Url) extends RenderedPage {

    override def links: Set[Url] = {
      Jsoup.parse(content, webEngine.getLocation)
        .select("a")
        .map(a => a.attr("abs:href").trim)
        .filter(_.nonEmpty)
        .map(link => Url(link))
        .toSet
    }

    override def content: String = {
      val src = new DOMSource(webEngine.getDocument)
      val writer = new StringWriter()
      val transformer = TransformerFactory.newInstance().newTransformer()
      transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8")
      transformer.transform(src, new StreamResult(writer))
      writer.flush()
      writer.toString
    }

  }

}

object BrowserRegion {

  private val pool = mutable.HashSet.empty[BrowserRegion]
  private val awaiting = mutable.ListBuffer.empty[Promise[BrowserRegion]]

  private[browser] def settings(region: BrowserRegion): BrowserSettings = synchronized {
    release(region)
    Browser.defaultSettings
  }

  private[browser] def get()(implicit exec: ExecutionContext): Future[BrowserRegion] = synchronized {
    val promise = Promise[BrowserRegion]()
    if (pool.isEmpty) {
      spawn(BrowserWebView.run(newId, true)).onFailure { case e => e.printStackTrace() }
      awaiting += promise
    } else {
      val region = pool.head
      promise.success(region)
      pool.remove(region)
    }
    promise.future
  }

  private[browser] def release(region: BrowserRegion): Unit = synchronized {
    if (awaiting.nonEmpty) awaiting.remove(0).success(region)
    else pool += region
  }

  private def newId = Random.alphanumeric.take(32).mkString

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