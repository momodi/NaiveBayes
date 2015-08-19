/**
 * Created by gaoyunxiang on 4/20/15.
 */


import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable
import scala.collection.immutable
import MyCommon._
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable._
object BayesPredict {

    def cal_test(input:String, pwz_broadcase:Broadcast[immutable.Map[Long, immutable.Map[Int, Int]]], pz:immutable.Map[Int, Double], lambda:Double) {
        val pz_sum = pz.map(_._2).sum

        val test = SparkCommon.sc.textFile(input, 100).coalesce(100).mapPartitions { ones =>
            val pwz_b = pwz_broadcase.value
            ones.grouped(100).flatMap { lines =>
                lines.par.map { line =>
                    val sp = line.split("\t")
                    val item = sp(0).myhash()
                    val cag = sp(1).toInt
                    val pzi = mutable.HashMap[Int, Double]().withDefaultValue(0.0)

                    sp(2).split("@").foreach { w =>
                        val whash = w.myhash()
                        pz.foreach {
                            case (z, zc) => {
                                if (pwz_b.contains(whash) && pwz_b(whash).contains(z)) {
                                    pzi(z) += math.log(1.0 * pwz_b(whash)(z) / pz(z))
                                } else {
                                    pzi(z) += math.log(1.0 * lambda / pz(z))
                                }
                            }
                        }
                    }
                    val sorted = pzi.toArray.sortBy { case (k, v) =>
                        -(v + math.log(1.0 * pz(k) / pz_sum))
                    }
                    if (sorted.isEmpty) {
                        (0, 0, 1)
                    } else if (sorted(0)._1 == cag) {
                        (1, 1, 1)
                    } else {
                        (0, 1, 1)
                    }
                }
            }
        }.reduce { case ((a1, a2, a3), (b1, b2, b3)) =>
            (a1 + b1, a2 + b2, a3 + b3)
        }
        println("precision: %d %d %d %f %f".format(test._1, test._2, test._3, test._1 * 100.0 / test._2, test._1 * 100.0 / test._3))
    }
    def main(args:Array[String]): Unit = {
        val lambda = args(0).toDouble
        val bayes_pz_input = args(1)
        val bayes_pwz_input = args(2)
        val test_input = args(3)
        val pz = SparkCommon.sc.textFile(bayes_pz_input).map { line =>
            val sp = line.split("\t")
            (sp(0).toInt, sp(1).toDouble)
        }.collect().toMap
        val pwz = SparkCommon.sc.textFile(bayes_pwz_input).map { line =>
            val sp = line.split("\t")
            val w = sp(0).myhash()
            val p = sp(1).split("#").map { one =>
                val one_sp = one.split("@")
                val z = one_sp(0).toInt
                (z, one_sp(1).toInt)
            }.toMap
            (w, p)
        }.collect().toMap

        val pwz_broadcase = SparkCommon.sc.broadcast(pwz)

        cal_test(test_input, pwz_broadcase, pz, lambda)


    }
}
