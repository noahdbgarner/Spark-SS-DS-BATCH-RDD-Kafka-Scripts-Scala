import java.time.LocalDateTime
import java.util.regex.Pattern


//now lets continue, we have a Date, and a Timestamp, oh but our Timestamp is off. This is bad
//Its because milliseconds is not being treated as fractions of a second, but as
//958307 milliseconds which is > 30 minutes. This throws everything off.... I see
//How to fix this???


val timestamp = "(\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\.\\d{3})"
//match only letters after the letters I, D, or E.
//now we don't even have to use split or drop or anything! magic!
//grab everything, then replace the numbers with ""
val messageAfterIDE = "((?<=I)(.*))"

val digits = "(\\d{5}\\s+\\d{5})"
//group(1) is timestamp
val regex = s"$timestamp $digits $messageAfterIDE"

val string = """10-22 18:26:34.352 12237 12285 I reconnect: [12237:547599925104:vendor/nest/reconnect/src/Media/ProxyMediaSystem.cpp(873)] Getting still image, time_ago_ms=40"""

//okay lets work with regex to find Date **-**
val dateMatcher = Pattern.compile(regex).matcher(string)

if (dateMatcher.find()) {
  println("hello")
  val dateString = "2019-" + dateMatcher
    .group(1)
    .replace(" ", "T")

  val localDateTime = LocalDateTime
    .parse(dateString)

  println(localDateTime.toString)

}

if (dateMatcher.find) {
  val messageString = dateMatcher
    .group(1)


  println(messageString)
}










