package com.comp.utils

import java.net.InetAddress

import com.maxmind.geoip2.DatabaseReader
import com.maxmind.geoip2.exception.AddressNotFoundException
import nl.basjes.parse.useragent.UserAgentAnalyzer
import org.joda.time.DateTimeZone
import org.joda.time.format.DateTimeFormat

case class WebLogUtils(ip: String, userAgents: String, time: String) {
  import WebLogUtils.ProcessIp._
  import WebLogUtils.ProcessUserAgent._

  def getCntry: String = parseCntry(InetAddress.getByName(ip))

  def getDeviceName: String = parseDeviceName(userAgents)

  def getTime: Long = parseTime(time)
}

object WebLogUtils {

  private object ProcessIp {
    @transient
    private lazy val db = getClass.getResourceAsStream("/maxmind/GeoLite2-Country.mmdb")

    @transient
    private lazy val dbReader = new DatabaseReader.Builder(db).build()

    def parseCntry(ip: InetAddress): String =
      try {
        dbReader.country(ip).getCountry.getIsoCode
      } catch {
        case _: AddressNotFoundException => s"$ip not found in db"
      }
  }

  private object ProcessUserAgent {
    @transient
    private lazy val formatter = DateTimeFormat.forPattern("[dd/MMM/yyyyH:m:s +0000]").withZone(DateTimeZone.UTC)
    @transient
    private lazy val uaa = UserAgentAnalyzer.newBuilder().withField("DeviceName").build()

    def parseDeviceName(ua: String): String = uaa.parse(ua).getValue("DeviceName")

    def parseTime(time: String): Long = formatter.parseDateTime(time).getMillis / 1000
  }
}
