package com.rockwell.twb.util

// very simple stop watch to avoid using Guava's one
// It is created just to check zookeerper offset commiting time
class Stopwatch {

  private val start = System.currentTimeMillis()

  override def toString() = (System.currentTimeMillis() - start) + " ms"

}
