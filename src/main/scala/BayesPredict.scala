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

    def main(args:Array[String]): Unit = {
        val lambda = args(0).toDouble
        val bayes_pz_input = args(1)
        val bayes_pwz_input = args(2)
        val test_input = args(3)
        val pz = SparkCommon.sc.textFile(bayes_pz_input).map { line =>
            val sp = line.split("\t")
            (sp(0).toInt, sp(1).toDouble)
        }.collect().toMap

        val test_w_item = SparkCommon.sc.textFile(test_input).flatMap { line =>
            val sp = line.split("\t")
            val item = sp(0).toLong
            val real_z = sp(1).toInt
            sp(2).split("@").slice(0, 100).distinct.map { w =>
                (w.myhash(), (item, real_z))
            }
        }.repartition(500)
        val pz_sum = pz.toIterator.map(_._2).sum

        val w_length = SparkCommon.sc.textFile(bayes_pwz_input).map { line =>
            val sp = line.split("\t")
            sp(0).myhash()
        }.distinct().count()


        val test_output = SparkCommon.sc.textFile(bayes_pwz_input, 500).map { line =>
            val sp = line.split("\t")
            val w = sp(0).myhash()
            val vec = sp(1).split("#").map { one =>
                val one_sp = one.split("@")
                val z = one_sp(0).toInt
                (z, one_sp(1).toInt + 1)
            }
            (w, vec)
        }.join(test_w_item).map {
            case (whash, (vec, (item, real_z))) =>
                ((item, real_z), vec)
        }.groupByKey().mapPartitions {
            case ones =>
                ones.grouped(100).flatMap {
                    each =>
                        each.par.map {
                            case ((item, real_z), iter) =>
                                val pzi = mutable.HashMap[Int, Double]().withDefaultValue(0.0)
                                iter.foreach {
                                    case one =>
                                        val one_dict = one.toMap
                                        pz.foreach {
                                            case (z, zc) =>
                                                pzi(z) += math.log((lambda + one_dict.getOrElse(z, 0)) / (pz(z) + w_length * lambda))
                                        }
                                }
                                val sorted = pzi.toArray.sortBy { case (k, v) =>
                                    -(v + math.log(1.0 * pz(k) / (lambda * w_length * pz.size + pz_sum)))
                                }
                                if (sorted.head._2 == sorted.last._2) {
                                    (0, 0, 1)
                                } else if (sorted.head._1 == real_z) {
                                    (1, 1, 1)
                                } else {
                                    (0, 1, 1)
                                }
                        }
                }
        }.reduce { case ((a1, a2, a3), (b1, b2, b3)) =>
            (a1 + b1, a2 + b2, a3 + b3)
        }
        println("precision: %d %d %d %f %f".format(test_output._1, test_output._2, test_output._3, test_output._1 * 100.0 / test_output._2, test_output._1 * 100.0 / test_output._3))


    }
}
