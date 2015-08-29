package com.github.lucastorri.moca.browser;

import javafx.application.Application;
import javafx.scene.Scene;
import javafx.stage.Stage;


public class BrowserWebView extends Application {

    @Override
    public void start(Stage stage) throws Exception {
        String id = getParameters().getRaw().get(0);
        BrowserRegion browser = new BrowserRegion(id);
        Scene scene = new Scene(browser, browser.settings().width(), browser.settings().height());
        stage.setTitle("Web View " + id);
        stage.setScene(scene);
        stage.show();
    }

    public static void run(String id, boolean headless) {
        if (headless) {
            System.setProperty("javafx.monocle.headless", "true");
            System.setProperty("glass.platform", "Monocle");
            System.setProperty("monocle.platform", "Headless");
            System.setProperty("prism.order", "sw");
            new ToolkitApplicationLauncher().launch(BrowserWebView.class, id);
        } else {
            launch(id);
        }
    }

}
