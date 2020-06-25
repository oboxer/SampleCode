package com.comp.etl.mock.database.model

import java.util.regex.{Matcher, Pattern}

case class WebLog(
    clientIp: String,
    rfc1413ClientIdentity: String,
    userName: String,
    dateTime: String,
    request: String,
    httpStatusCode: String,
    bytesSent: String,
    referer: String,
    userAgent: String,
)

object WebLog {
  private val ddd = "\\d{1,3}"
  private val ip = s"($ddd\\.$ddd\\.$ddd\\.$ddd)?"
  private val client = "(\\S+)"
  private val user = "(\\S+)"
  private val dateTime = "(\\[.+?\\])"
  private val request = "\"(.*?)\""
  private val status = "(\\d{3})"
  private val bytes = "(\\S+)"
  private val referer = "\"(.*?)\""
  private val agent = "\"(.*?)\""
  private val regex = s"$ip $client $user $dateTime $request $status $bytes $referer $agent"
  private val p = Pattern.compile(regex)

  private def buildAccessLogRecord(matcher: Matcher): WebLog =
    WebLog(
      matcher.group(1),
      matcher.group(2),
      matcher.group(3),
      matcher.group(4),
      matcher.group(5),
      matcher.group(6),
      matcher.group(7),
      matcher.group(8),
      matcher.group(9),
    )

  def parse(webLog: String): Option[WebLog] = {
    val matcher = p.matcher(webLog)
    if (matcher.find) Some(buildAccessLogRecord(matcher)) else None
  }
}
