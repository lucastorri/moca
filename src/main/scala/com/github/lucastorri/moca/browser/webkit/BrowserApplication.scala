package com.github.lucastorri.moca.browser.webkit

import javafx.application.{Application, Platform}
import javafx.scene.Scene
import javafx.stage.Stage

import com.github.lucastorri.moca.async.runnable

class BrowserApplication extends Application {

  override def start(stage: Stage): Unit =
    BrowserWindow.register(this)

  def newWindow(settings: WebKitSettings): Unit = {
    Platform.runLater(runnable {
      val stage = new Stage()
      val window = new BrowserWindow(settings, stage)
      val scene = new Scene(window, settings.width, settings.height)
      stage.setScene(scene)
      stage.show()
      BrowserWindow.release(window)
    })
  }

}
