package smr.storage

import java.io.{ObjectInputStream, ObjectOutputStream}


/**
 * 
 * @author dlwh
 */

trait Serialization {
  def write[T](output: ObjectOutputStream, t: T)
  def read[T](input: ObjectInputStream):T

}

class AkkaSerialization(val akkaSerialization: akka.serialization.Serialization) extends Serialization {
  private val TagInt:Byte = 0
  private val TagLong:Byte = 1
  private val TagShort:Byte = 2
  private val TagDouble:Byte = 3
  private val TagFloat:Byte = 4
  private val TagChar:Byte = 5
  private val TagByte:Byte = 6
  private val TagOther:Byte = 7

  def write[T](output: ObjectOutputStream, t: T) = t match {
    case t: Int => output.writeByte(TagInt); output.writeInt(t)
    case t: Long => output.writeByte(TagLong); output.writeLong(t)
    case t: Short => output.writeByte(TagShort); output.writeShort(t)
    case t: Double => output.writeByte(TagDouble); output.writeDouble(t)
    case t: Float => output.writeByte(TagFloat); output.writeFloat(t)
    case t: Char => output.writeByte(TagChar); output.writeChar(t)
    case t: Byte => output.writeByte(TagByte); output.writeByte(t)
    case t: AnyRef =>
      output.writeByte(TagOther)
      val s = akkaSerialization.findSerializerFor(t)
      if(s.includeManifest) {
        output.writeBoolean(true)
        output.writeObject(t.getClass)
      } else {
        output.writeBoolean(false)
      }
      output.write(s.identifier)
      val arr = s.toBinary(t)
      output.writeInt(arr.length)
      output.write(arr)
  }

  def read[T](input: ObjectInputStream) = {
    input.readByte() match {
      case TagInt => input.readInt()
      case TagLong => input.readLong()
      case TagShort => input.readShort()
      case TagDouble => input.readDouble()
      case TagFloat => input.readFloat()
      case TagChar => input.readChar()
      case TagByte => input.readByte()
      case TagOther =>
        val manifest = if(input.readBoolean()) Some(input.readObject().asInstanceOf[Class[T]]) else None
        val id = input.readInt()
        val length = input.readInt()
        val bytes = new Array[Byte](length)
        input.read(bytes)
        akkaSerialization.deserialize(bytes, id, manifest) match {
          case Left(t) => throw t
          case Right(t) => t
        }

    }
  }.asInstanceOf[T]

}