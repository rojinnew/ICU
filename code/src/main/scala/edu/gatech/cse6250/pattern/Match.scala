package edu.gatech.cse6250.pattern

import java.util.regex.Pattern

 object Match {

  def extractDate(str: String) = {

    //[\[\]0-9*-]+ at ([0-9:]+
   // val j = "Date/Time: [**2174-4-12**] at 09:07"
    var res = ""
    val pattern = "([\\s\\S]*)(Date\\/Time:) (\\[\\*\\*[0-9]+\\-[0-9]+\\-[0-9]+\\*\\*\\]) (at) ([0-9]+:[0-9]+)([\\s\\S]*)"//.r
    val x = pattern.r
    val pattern2 = Pattern.compile(pattern)
    val matcher2 = pattern2.matcher(str)

    //println(matcher2.find())
    if (matcher2.find())
    {
     // println("yes")
      val x(zero, one, two, three, four,five) = str
      res = two.replace("*", "").replace("[", "").replace("]", "") + " " + four.toString

      res
    }
    else
    {
      "null"
    }


  }


  def extractWeight(str: String) = {
    /*
          val pattern = "(Height:) (\\(in\\)) ([0-9]+)".r
          val pattern(count, fruit, height) = str
          height
    */
    //[\[\]0-9*-]+ at ([0-9:]+
    var res = ""
//Height (in): 67  Weight (lbs): 185
    val pattern = "([\\s\\S]*)  (Weight) (\\(lbs\\)\\:) ([0-9]+)([\\s\\S]*)"//.r
    val x = pattern.r
    val pattern2 = Pattern.compile(pattern)
    val matcher2 = pattern2.matcher(str)
    //println(matcher2.find())
    if (matcher2.find())
    {
      println("yes")
      val x(zero, one, two, three, four) = str
      res = three.toString

      res.toDouble
    }
    else
    {
      -1.0
    }


  }



}
