package edu.monash.IRM.Test

import java.net.InetSocketAddress
import java.nio.channels.{SelectionKey, Selector, ServerSocketChannel}

import scala.io.BufferedSource

/**
 * Created by psangat on 17/12/15.
 */
object MultithreadServer {

  def main(args: Array[String]) {
    val selector = Selector.open()

    val ports = Array(9999, 9998, 9997)

    ports.foreach {
      port =>
        val serverChannel = ServerSocketChannel.open()
        serverChannel.configureBlocking(false)
        serverChannel.socket().bind(new InetSocketAddress(port))
        serverChannel.register(selector, SelectionKey.OP_ACCEPT)

    }

    while (true) {
      selector.select()
      val selectedKeys = selector.selectedKeys().iterator()
      while (selectedKeys.hasNext()) {
        val selectedKey = selectedKeys.next()
        if (selectedKey.isAcceptable()) {
          val socketChannel = selectedKey.channel().asInstanceOf[ServerSocketChannel].accept()
          // socketChannel.configureBlocking(false)
          println("Port REcoev: " + socketChannel.socket().getLocalPort())
          socketChannel.socket().getLocalPort() match {
            case 9999 =>
              val in = new BufferedSource(socketChannel.socket().getInputStream()).getLines()
              in.foreach { item => println("[Port 9999]: " + item) }
              socketChannel.socket().close()
            case 9998 =>
              val in = new BufferedSource(socketChannel.socket().getInputStream()).getLines()
              in.foreach { item => println("[Port 9998]: " + item) }
              socketChannel.socket().close()
            case 9997 =>
              val in = new BufferedSource(socketChannel.socket().getInputStream()).getLines()
              in.foreach { item => println("[Port 9997]: " + item) }
              socketChannel.socket().close()

            case _ => println("Port not defined")

          }
        }
      }
    }
  }

}
