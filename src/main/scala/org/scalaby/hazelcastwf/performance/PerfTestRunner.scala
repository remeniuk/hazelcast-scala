package org.scalaby.hazelcastwf.performance

import com.google.caliper.runner.CaliperMain
import com.google.caliper.Runner

/*import com.google.caliper.Runner
import java.util.{Properties, HashMap}
import scala.collection.JavaConverters._
import java.io.{FileInputStream, StringWriter, PrintWriter, File}
import java.net.URLClassLoader
import com.google.caliper.runner._*/

/**
 * User: remeniuk
 */

object PerfTestRunner {

  def main(args: Array[String]) = {
    Runner.main(classOf[DistributedTaskPerfTest], Array[String]():_*)
  }

  /*
  def main(args: Array[String]) = {
    //CaliperMain.main(classOf[DistributedTaskPerfTest], args)
    val options = ParsedOptions.from(Array[String](
      "--logging",
      "--verbose",
      "--output", "/home/remeniuk/.caliper-results",
      "org.scalaby.hazelcastwf.performance.DistributedTaskPerfTest"))

    //println(options.verbose())
    //println(options.outputFileOrDir())
    //println(System.getProperty("java.class.path"))
    //println("CLASSPATH = " + ClassLoader.getSystemClassLoader().asInstanceOf[URLClassLoader].getURLs.toList.mkString(":"))

    System.setProperty("java.class.path", "/home/remeniuk/projects/hazelcast-scala/target/scala-2.9.1/classes:/home/remeniuk/projects/hazelcast-scala/lib/caliper-1.0-LOCAL.jar:/home/remeniuk/.sbt/boot/scala-2.9.1/lib/scala-library.jar:/home/remeniuk/.ivy2/cache/javax.servlet/servlet-api/jars/servlet-api-2.5.jar:/home/remeniuk/.ivy2/cache/com.hazelcast/hazelcast/jars/hazelcast-1.9.4.4.jar:/home/remeniuk/.ivy2/cache/org.specs2/specs2_2.9.1/jars/specs2_2.9.1-1.6.1.jar:/home/remeniuk/.ivy2/cache/org.specs2/specs2-scalaz-core_2.9.1/jars/specs2-scalaz-core_2.9.1-6.0.1.jar:/home/remeniuk/.ivy2/cache/org.scalaz/scalaz-core_2.9.1/jars/scalaz-core_2.9.1-6.0.3.jar:/home/remeniuk/.ivy2/cache/com.google.guava/guava/jars/guava-10.0.1.jar:/home/remeniuk/.ivy2/cache/com.google.code.findbugs/jsr305/jars/jsr305-1.3.9.jar:/home/remeniuk/.ivy2/cache/com.google.code.gson/gson/jars/gson-2.1.jar")

    val stringWriter = new StringWriter()
    val writer = new PrintWriter(stringWriter)
    var console = new DefaultConsoleWriter(writer)

    val props = new Properties()
    //getClass.getResourceAsStream("global.calipercc")
    props.load(new FileInputStream("/home/remeniuk/projects/hazelcast-scala/src/main/resources/global.caliperrc"))
    val map = props.entrySet.asScala.map{e =>
      e.getKey.toString -> e.getValue.toString
    } toMap
    var rc = new CaliperRc(map.asJava)//CaliperRcManager.loadOrCreate(new File(""))

    val caliper = new CaliperRun(options, rc, console)
    val data = caliper.run()

    data.scenarios.asScala.foreach{scen =>
      println("""
        class: %s
        method: %s
        params: %s
      """ format(scen.benchmarkClassName, scen.benchmarkMethodName, scen.userParameters))
    }

    data.results.asScala.map(new SimplyProcessedResult(_)).foreach { result =>
      println("""
      NAME: %s
      MEAN: %s
      """ format(result.getResponseDesc, result.getMean))
      //result.measurements.asScala.foreach{measurment =>
      //}
    }
    //writer.flush()
    //stringWriter.flush()
    //println("OUT == " + stringWriter.toString)
  }
    */
}