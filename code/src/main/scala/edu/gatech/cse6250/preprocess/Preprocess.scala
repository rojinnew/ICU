/**
  * @author Rojin <rojin@gatech.edu>
 */
package edu.gatech.cse6250.preprocess
import java.io.{File, PrintWriter}
import scala.io.Source
object Preprocess {
  def process()  : Unit={

    val path = "dataset/"


    val f2 = new File(path + "temp.csv"); // temp file
    if (!f2.exists()) {
      println("cerate a temp lab events file")
      f2.createNewFile();
    }
    else {
      println("temporary labe events file already exits")

    }
    val f1 = new File(path + "LABEVENTS.csv") //original file
    val w = new PrintWriter(f2)
    Source.fromFile(f1).getLines
           .map(x => x.replace("\"\"\"", "\""))
           .foreach(x => w.println(x))
    w.close()
    f2.renameTo(f1)
  }

}


