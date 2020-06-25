package com.comp.etl.mock.database.model

import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

class WebLogTest extends AsyncFreeSpec with Matchers {

  "WebLog" - {
    "parse" - {
      "should have valid results" in {
        val input =
          "202.182.74.252 - 12309.rfihjo [23/Jun/202006:56:31 +0000] \"PUT /categories HTTP/1.0\" 100 3249 \"http://www.perez.info/search/\" \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_5_2) AppleWebKit/536.2 (KHTML, like Gecko) Chrome/29.0.852.0 Safari/536.2\""
        val actual = WebLog.parse(input).get

        assert(actual.clientIp == "202.182.74.252")
        assert(actual.userName == "12309.rfihjo")
        assert(actual.dateTime == "[23/Jun/202006:56:31 +0000]")
        assert(
          actual.userAgent == "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_5_2) AppleWebKit/536.2 (KHTML, like Gecko) Chrome/29.0.852.0 Safari/536.2",
        )
      }
    }
  }
}
