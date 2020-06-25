package com.comp.utils

import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

class WebLogUtilsTest extends AsyncFreeSpec with Matchers {

  "WebLogUtils" - {
    val input = WebLogUtils(
      "202.182.74.252",
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_5_2) AppleWebKit/536.2 (KHTML, like Gecko) Chrome/29.0.852.0 Safari/536.2",
      "[23/Jun/202023:24:26 +0000]",
    )

    "ProcessIp" - {
      "should parse ip into country" in {
        assert(input.getCntry == "AU")
      }
    }

    "ProcessUserAgent" - {
      "should parse user agent into device name" in {
        assert(input.getDeviceName == "Apple Macintosh")
      }
    }
  }
}
