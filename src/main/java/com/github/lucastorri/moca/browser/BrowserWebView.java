package com.github.lucastorri.moca.browser;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.scene.Scene;
import javafx.stage.Stage;


public class BrowserWebView extends Application {

    @Override
    public void start(Stage stage) throws Exception {
        BrowserRegion.register(this);
    }

    public void newWindow(BrowserSettings settings) {
        Platform.runLater(() -> {
            BrowserRegion browser = new BrowserRegion(settings);
            Scene scene = new Scene(browser, settings.width(), settings.height());
            Stage stage = new Stage();
            stage.setScene(scene);
            stage.show();
        });
    }

    public static void start(boolean headless) {
        if (headless) {
            System.setProperty("javafx.monocle.headless", "true");
            System.setProperty("glass.platform", "Monocle");
            System.setProperty("monocle.platform", "Headless");
            System.setProperty("prism.order", "sw");
            new ToolkitApplicationLauncher().launch(BrowserWebView.class);
        } else {
            launch();
        }
    }

}
