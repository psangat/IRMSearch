package edu.monash.Thesis

/**
 * Created by psangat on 12/09/15.
 */


import java.util.ArrayList

import scala.collection.JavaConversions._

case class Node(
                 operation: String,
                 children: ArrayList[Node],
                 var value: String,
                 weight: Int
                 ) {

  def isLeaf = if (children.size() <= 0) true else false
}

object BBTree {
  val leaves = new ArrayList[String]

  def createTree(): Node = {
    val w1 = new Node("", new ArrayList[Node], "University", 1)
    val w2 = new Node("", new ArrayList[Node], "Monash", 2)
    val w3 = new Node("", new ArrayList[Node], "1958", 3)
    val w4 = new Node("", new ArrayList[Node], "oldest", 4)
    val w5 = new Node("", new ArrayList[Node], "founded", 5)

    val orChild = new ArrayList[Node]
    orChild.add(w1)
    orChild.add(w2)
    orChild.add(w3)
    val orNode = new Node("OR", orChild, "", -1)

    val andChild = new ArrayList[Node]
    andChild.add(w4)
    andChild.add(w5)
    val andNode = new Node("AND", andChild, "", -1)

    val notChild = new ArrayList[Node]
    notChild.add(andNode)
    val notNode = new Node("NOT", notChild, "", -1)

    val rootChild = new ArrayList[Node]
    rootChild.add(orNode)
    rootChild.add(notNode)

    val root = new Node("And", rootChild, "", -1)
    return root
  }

  // returns all the leaves of the tree
  def getAllLeaves(node: Node): ArrayList[String] = {
    if (node.isLeaf)
      leaves.add(node.value)
    else if (node.operation.equalsIgnoreCase("NOT"))
      getAllLeaves(node.children.get(0))
    else if (node.operation.equalsIgnoreCase("AND")) {
      node.children.foreach { child =>
        getAllLeaves(child)
      }
    }
    else if (node.operation.equalsIgnoreCase("OR")) {
      node.children.foreach {
        child =>
          getAllLeaves(child)
      }
    }
    return leaves
  }

  // replaces all the leaves of tree with its corresponding xTraps
  def replaceAllLeaves(node: Node): Node = {
    if (node.isLeaf) {
      val xTrap = Common.hash("F", "KX", node.value)
      node.value = xTrap.toString
    }
    else if (node.operation.equalsIgnoreCase("NOT"))
      replaceAllLeaves(node.children.get(0))
    else if (node.operation.equalsIgnoreCase("AND")) {
      node.children.foreach { child =>
        replaceAllLeaves(child)
      }
    }
    else if (node.operation.equalsIgnoreCase("OR")) {
      node.children.foreach {
        child =>
          replaceAllLeaves(child)
      }
    }
    return node
  }

}
