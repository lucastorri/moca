package com.github.lucastorri.moca.criteria

case object AHrefCriteria extends JavaScriptCriteria {

  override val script: String =
    "Array.prototype.slice.call(document.getElementsByTagName('a')).map(function(e) { return e.href; });"

}




