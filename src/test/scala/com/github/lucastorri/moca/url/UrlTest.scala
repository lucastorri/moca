package com.github.lucastorri.moca.url

import org.scalatest.{FlatSpec, MustMatchers}

class UrlTest extends FlatSpec with MustMatchers {

  it must "remove hash from url" in {

    Url("http://www.example.com/test#hi").toString must equal ("http://www.example.com/test")

  }

  it must "identify ports and protocol" in {

    Url("http://www.example.com/").port must equal (80)
    Url("http://www.example.com/").protocol must equal ("http")

    Url("https://www.example.com/").port must equal (443)
    Url("https://www.example.com/").protocol must equal ("https")

    Url("http://www.example.com:8080/").port must equal (8080)
    Url("http://www.example.com:8080/").protocol must equal ("http")

  }

  it must "return host and domain" in {

    Url("http://www.example.com/").host must equal ("www.example.com")
    Url("http://www.example.com/").domain must equal ("example.com")

    Url("http://www.fazenda.gov.br/").host must equal ("www.fazenda.gov.br")
    Url("http://www.fazenda.gov.br/").domain must equal ("fazenda.gov.br")

  }

  it must "normalize paths" in {

    Url("http://www.example.com/a/b/../c").toString must equal ("http://www.example.com/a/c")

  }

  it must "resolve paths" in {

    Url("http://www.example.com/k/x").resolve("y/z").toString must equal ("http://www.example.com/k/y/z")
    Url("http://www.example.com/k/x/").resolve("y/z").toString must equal ("http://www.example.com/k/x/y/z")
    Url("http://www.example.com/k/x").resolve("/y/z").toString must equal ("http://www.example.com/y/z")

  }

  it must "not allow allow protocols other than http and https" in {

    intercept[InvalidUrlException] {
      Url("javascript:;")
    }

    intercept[InvalidUrlException] {
      Url("ftp://example.com")
    }

    Url("HTTP://www.example.com")

  }

  it must "remove trailing ?" in {

    Url("http://www.example.com/a?").toString must equal ("http://www.example.com/a?")

  }

}
