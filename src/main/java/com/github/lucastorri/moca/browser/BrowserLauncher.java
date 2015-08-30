package com.github.lucastorri.moca.browser;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.scene.Scene;
import javafx.stage.Stage;


public class BrowserLauncher {

    public static void launch(boolean headless) {
        Class<BrowserApplication> appClass = BrowserApplication.class;
        if (headless) {
            System.setProperty("javafx.monocle.headless", "true");
            System.setProperty("glass.platform", "Monocle");
            System.setProperty("monocle.platform", "Headless");
            System.setProperty("prism.order", "sw");
            new ToolkitApplicationLauncher().launch(appClass);
        } else {
            Application.launch(appClass);
        }
    }

}
