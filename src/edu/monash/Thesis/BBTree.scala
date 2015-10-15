package edu.monash.Thesis

/**
 * Created by psangat on 16/10/15.
 */

import java.util.ArrayList

case class Node(
                 value: String,
                 level: Int,
                 children: ArrayList[Node],
                 operation: String,
                 weight: Int
                 ) {

  def isLeaf = if (children.size() <= 0) true else false
}

object BBTree {

  def createTree(): Unit = {
    val root = new Node()

  }

}
