package edu.gatech.cse6250.plot
import breeze.plot._
import breeze.linalg._

 object Plot {


    def create_plot(fpr:Array[Double],tpr:Array[Double] ,name:String, chartname:String)
    {
      val x = linspace(0.0,1.0)
      val f1 = Figure()
      val p1 = f1.subplot(0)
      p1 += plot(fpr, tpr)
      p1 += plot(x, x,'.')
      //plot(Array(0,0.5,1),Array(0,0.5,1) )
      p1.title_=("ROC Curve - "+chartname)
      p1.xlabel = "false positives rate"
      p1.ylabel = "true positives rate"
      p1.xlim = (0.0, 1.0)
      p1.ylim = (0.0, 1.0)
      //p1.xaxis.setTickUnit(new NumberTickUnit(0.01))
      // p1.yaxis.setTickUnit(new NumberTickUnit(0.1))
      f1.refresh
      f1.saveas(name)
    }





}
