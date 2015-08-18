import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
 * Created by gaoyunxiang on 6/9/14.
 * 一些简单的hash函数、离散化函数和配置文件
 */

object MyCommon {
    implicit class HashImprovements(val str: String) {
        /** 对一个字符串做64bit的hash */
        def myhash(shift:Int = 0):Long = {
            str.foldLeft(2166136261l + shift)((x, c) => (16777619 * x) ^ c.toLong)
        }
    }
}
