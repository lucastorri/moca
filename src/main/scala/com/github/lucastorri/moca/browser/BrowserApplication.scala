package com.github.lucastorri.moca.browser

import javafx.application.{Application, Platform}
import javafx.scene.Scene
import javafx.stage.Stage

import com.github.lucastorri.moca.async.runnable

class BrowserApplication extends Application {

  override def start(stage: Stage): Unit =
    BrowserRegion.register(this)

  def newWindow(settings: BrowserSettings): Unit = {
    Platform.runLater(runnable {
      val browser = new BrowserRegion(settings)
      val scene = new Scene(browser, settings.width, settings.height)
      val stage = new Stage()
      stage.setScene(scene)
      stage.show()
      BrowserRegion.release(browser)
    })
  }

}
