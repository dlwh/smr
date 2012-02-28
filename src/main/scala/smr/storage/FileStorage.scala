package smr.storage

import java.util.zip.{GZIPInputStream, GZIPOutputStream}
import java.io._
import smr.Shard


/**
 * 
 * @author dlwh
 */

trait FileStorage extends Storage {
  val directory: File
  val serialization: Serialization
  private var anonCount = 0
  private def nextId = synchronized {
    anonCount += 1
    anonCount
  }


  def store[T](name: String, t: T): Shard = {
    val file = new File(directory, name)
    file.mkdirs()
    val oostream = new ObjectOutputStream(new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(file))))
    serialization.write(oostream,t)
    oostream.close()
    Shard(file.toURI)
  }

  def load[T](uri: Shard) = {
    val file = new File(uri.uri)
    if(!file.exists) None
    else {
      val istream = new ObjectInputStream(new GZIPInputStream(new BufferedInputStream(new FileInputStream(file))))
      val cc =  {
        val id = istream.readInt()
        val man = if(istream.readBoolean()) {
          Some(istream.readObject().asInstanceOf[Class[_<:T]])
        } else {
          None
        }
        val length = istream.readInt()
        val in = new Array[Byte](length)
        istream.readFully(in)
        serialization.read(istream)
      }
      istream.close()
      Some(cc)
    }
  }

  def store[T](t: T): Shard = {
    store("_anon/piece-"+nextId, t)
  }


  def forget(uri: Shard) = {
    val file = new File(uri.uri)
    if(file.exists) {
      file.delete()
    }
  }

  def name(shards: IndexedSeq[Shard], name: String) = {
    new File(directory,name).mkdirs()
    for( (shard,i) <- shards.toArray.zipWithIndex) yield {
      val uri = shard.uri
      val newFile = new File(directory,name + "/"+name+"-"+i)
      Runtime.getRuntime.exec(Array("ln",new File(uri).toString,newFile.toString))
      new Shard(newFile.toURI)
    }
  }

  def shardsFor(name: String) = {
    val file = new File(directory, name)
    if(!file.exists) None
    else Some(new File(directory,name).listFiles().map(x => Shard(x.toURI)))
  }
}


