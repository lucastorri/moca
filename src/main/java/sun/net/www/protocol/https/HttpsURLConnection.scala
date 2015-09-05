package sun.net.www.protocol.https

import java.net.{Proxy, URL}

class HttpsURLConnection(url: URL, proxy: Proxy, handler: sun.net.www.protocol.https.Handler)
  extends HttpsURLConnectionImpl(url, proxy, handler) {

}
