/**
 * Created by gaoyunxiang on 4/20/15.
 */
import scala.collection.mutable

object Bayes {
    def main(args:Array[String]): Unit = {
        val training_input = args(0)
        val bayes_pz_output = args(1)
        val bayes_pwz_output = args(2)
        val ori = SparkCommon.sc.textFile(training_input, 500).map { line =>
            val sp = line.split("\t")
            (sp(1).toInt, sp(2))
        }.cache()
        ori.map { case (cag, features) =>
            (cag, 1)
        }.reduceByKey { (a, b) =>
            a + b
        }.map { case (k, v) =>
            "%s\t%d".format(k, v)
        }.repartition(1).saveAsTextFile(bayes_pz_output)

        ori.mapPartitions { case ones =>
            val dict = mutable.HashMap[(String, Int), Int]().withDefaultValue(0)
            ones.foreach { case (cag, features) =>
                features.split("@").distinct.foreach { w =>
                    val k = (w, cag)
                    dict(k) = dict(k) + 1
                }
            }
            dict.toIterator
        }.reduceByKey { (a, b) =>
            a + b
        }.filter { case ((w, cag), v) =>
            v > 10
        }.groupBy { case ((w, z), v) =>
            w
        }.map { case (w, ps) =>
            val p = ps.map { case ((ww, z), v) =>
                (z, v)
            }.toArray.sortBy { case (z, v) =>
                -v
            }.map { case (z, v) =>
                "%d@%d".format(z, v)
            }
            "%s\t%s".format(w, p.mkString("#"))
        }.repartition(1).saveAsTextFile(bayes_pwz_output)
    }
}
