package com.github.lucastorri.moca.browser.webkit

import java.io.StringWriter
import java.net._
import java.nio.ByteBuffer
import java.nio.file.Files
import java.util.concurrent.{Executors, TimeUnit}
import javafx.application.Platform
import javafx.beans.value.{ChangeListener, ObservableValue}
import javafx.concurrent.Worker.State
import javafx.concurrent.{Worker => JFXWorker}
import javafx.geometry.{HPos, VPos}
import javafx.scene.layout.Region
import javafx.scene.web.WebView
import javafx.stage.Stage
import javax.xml.transform.dom.DOMSource
import javax.xml.transform.stream.StreamResult
import javax.xml.transform.{OutputKeys, TransformerFactory}

import com.github.lucastorri.moca.async.{runnable, spawn}
import com.github.lucastorri.moca.browser.webkit.net._
import com.github.lucastorri.moca.browser.{BrowserSettings, Content, JavascriptNotSupportedException, RenderedPage}
import com.github.lucastorri.moca.url.Url
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.IOUtils

import scala.collection.mutable
import scala.compat.Platform.currentTime
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Random

class BrowserWindow private[browser](settings: WebKitSettings, stage: Stage) extends Region with Ordered[BrowserWindow] with StrictLogging {

  val id = Random.alphanumeric.take(32).mkString
  private val browser = new WebView
  private val webEngine = browser.getEngine
  private var current: Url = _
  private var promise: Promise[RenderedPage] = _
  private var lastUsed = 0L

  logger.trace(s"Window $id starting")
  getChildren.add(browser)
  webEngine.setUserAgent(settings.base.userAgent)
  webEngine.setJavaScriptEnabled(settings.enableJavaScript)
  webEngine.getLoadWorker.stateProperty().addListener(new ChangeListener[State] {
    override def changed(event: ObservableValue[_ <: State], oldValue: State, newValue: State): Unit = {
      val url = Option(webEngine.getLocation).filter(_.trim != "about:blank")
      if (url.isDefined && event.getValue == JFXWorker.State.SUCCEEDED) {
        promise.success(InternalRenderedPage(current))
      }
    }
  })

  def goTo(url: Url): Future[RenderedPage] = {
    logger.trace(s"Window $id goTo $url")
    lastUsed = currentTime
    val pagePromise = Promise[RenderedPage]()
    Platform.runLater(runnable {
      this.current = url
      this.promise = pagePromise
      webEngine.load(url.toString)
    })
    pagePromise.future
  }

  private[BrowserWindow] def stop() = {
    Platform.runLater(runnable {
      webEngine.load(null)
    })
  }

  protected override def layoutChildren(): Unit =
    layoutInArea(browser, 0, 0, getWidth, getHeight, 0, HPos.CENTER, VPos.CENTER)

  protected override def computePrefWidth(height: Double): Double =
    settings.width

  protected override def computePrefHeight(width: Double): Double =
    settings.height

  case class InternalRenderedPage(originalUrl: Url) extends RenderedPage {

    override def renderedUrl: Url =
      Url.parse(webEngine.getLocation).getOrElse(originalUrl)

    override def exec(javascript: String): AnyRef =
      throw JavascriptNotSupportedException("JS disabled because bugs on javafx-webkit are causing the jvm to break")

    override def renderedHtml: String = {
      val src = new DOMSource(webEngine.getDocument)
      val writer = new StringWriter()
      val transformer = TransformerFactory.newInstance().newTransformer()
      transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8")
      transformer.transform(src, new StreamResult(writer))
      writer.flush()
      writer.toString
    }

    override def renderedContent: Content = {
      val buffer = settings.charset.encode(renderedHtml)
      val entry = BrowserWindow.cache.get(renderedUrl)
          .orElse(BrowserWindow.cache.get(originalUrl))
          .getOrElse(EmptyCacheEntry)

      Content(entry.status, entry.headers, buffer)
    }

    override def originalContent: Content = {
      val entry = BrowserWindow.cache.get(originalUrl)
        .getOrElse(EmptyCacheEntry)

      Content(entry.status, entry.headers, ByteBuffer.wrap(IOUtils.toByteArray(entry.content)))
    }
    
    override def settings: BrowserSettings =
      BrowserWindow.this.settings.base
  }

  override def compare(that: BrowserWindow): Int =
    that.lastUsed.compareTo(this.lastUsed)

  def close(): Unit = {
    stage.close()
  }

}

object BrowserWindow extends StrictLogging {

  private val cleanInterval = 10.minutes
  private val pool = mutable.TreeSet.empty[BrowserWindow]
  private val awaiting = mutable.ListBuffer.empty[Promise[BrowserWindow]]
  private val app = Promise[BrowserApplication]()
  private val cache: Cache = new FSCache(Files.createTempDirectory("moca-webkit"), 16384)
  
  URL.setURLStreamHandlerFactory(new MocaURLStreamHandlerFactory(cache))
  Platform.setImplicitExit(false)
  spawn {
    try BrowserLauncher.launch(WebKitBrowserProvider.settings.headless)
    catch { case e: Exception => logger.error("Could not start browser", e) }
  }

  private[browser] def register(view: BrowserApplication): Unit =
    app.success(view)

  private[browser] def get()(implicit exec: ExecutionContext): Future[BrowserWindow] = synchronized {
    val promise = Promise[BrowserWindow]()
    if (pool.isEmpty) {
      app.future.foreach(_.newWindow(WebKitBrowserProvider.settings))
      awaiting += promise
    } else {
      val window = pool.head
      pool.remove(window)
      promise.success(window)
    }
    promise.future
  }

  private[browser] def release(window: BrowserWindow): Unit = synchronized {
    logger.trace(s"Release ${window.id}")
    window.stop()
    if (awaiting.nonEmpty) awaiting.remove(0).success(window)
    else pool += window
  }

  private[this] val cleaningTask = runnable {
    BrowserWindow.synchronized {
      pool.takeWhile(window => (currentTime - window.lastUsed).millis > cleanInterval).foreach { window =>
        logger.info(s"Closing window ${window.id} for inactivity")
        window.close()
        pool.remove(window)
      }
    }
  }

  private[browser] def appLoaded(): Future[BrowserApplication] =
    app.future

  Executors.newScheduledThreadPool(1)
    .scheduleWithFixedDelay(cleaningTask, cleanInterval.toMillis, cleanInterval.toMillis, TimeUnit.MILLISECONDS)

}









