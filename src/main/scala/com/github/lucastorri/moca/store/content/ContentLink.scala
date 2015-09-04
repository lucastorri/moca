package com.github.lucastorri.moca.store.content

import com.github.lucastorri.moca.url.Url

case class ContentLink(url: Url, uri: String, depth: Int, hash: String)
