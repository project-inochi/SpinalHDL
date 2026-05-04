package spinal.tester.scalatest

import spinal.core._
import spinal.core.sim._
import spinal.lib._
import spinal.lib.bus.misc.SingleMapping
import spinal.lib.com.i2c._

object I2cSlaveBusSlaveFactoryTester {
  class Dut() extends Component {
    val io = new Bundle {
      val i2c = master(I2c())
    }

    val i2cCtrl = new I2cSlave(
      I2cSlaveGenerics(
        samplingWindowSize = 3,
        samplingClockDividerWidth = 10 bits,
        timeoutWidth = 20 bits
      )
    )

    val busCtrl = I2cSlaveBusSlaveFactory(
      i2cCtrl.io.bus,
      I2cSlaveBusSlaveFactoryConfig(slaveAddress = 0x42)
    )
    busCtrl.phase.simPublic()
    busCtrl.ack.pending.simPublic()
    busCtrl.ack.issued.simPublic()
    busCtrl.txByteLoaded.simPublic()
    busCtrl.txAwaitMasterAck.simPublic()
    busCtrl.rxByte.simPublic()

    i2cCtrl.io.config.samplingClockDivider := 3
    i2cCtrl.io.config.timeout := 25 * 20 - 1
    i2cCtrl.io.config.tsuData := 4
    i2cCtrl.io.config.timeoutClear := False

    io.i2c.scl.write := i2cCtrl.io.i2c.scl.write
    io.i2c.sda.write := i2cCtrl.io.i2c.sda.write
    i2cCtrl.io.i2c.scl.read := io.i2c.scl.read
    i2cCtrl.io.i2c.sda.read := io.i2c.sda.read

    val reg8 = busCtrl.createReadAndWrite(UInt(8 bits), 0x00)
    reg8.init(0x12)

    val reg16 = busCtrl.createWriteAndReadMultiWord(UInt(16 bits), 0x10)
    reg16.init(0x3456)

    val reg32 = busCtrl.createWriteAndReadMultiWord(UInt(32 bits), 0x20)
    reg32.init(U(BigInt("89ABCDEF", 16), 32 bits))

    val nextByte = Reg(UInt(8 bits)) init(0x66)
    nextByte := 0x66
    busCtrl.read(nextByte, 0x24)

    val readOnly = Reg(UInt(8 bits)) init(0x5A)
    readOnly := 0x5A
    busCtrl.read(readOnly, 0x30)

    val clearOnRead = Reg(Bits(8 bits)) init(B(0xA5, 8 bits))
    busCtrl.read(clearOnRead, 0x31)
    busCtrl.onRead(0x31) {
      clearOnRead := 0
    }

    val clearOnSet = Reg(Bits(8 bits)) init(B(0x0F, 8 bits))
    busCtrl.read(clearOnSet, 0x32)
    busCtrl.clearOnSet(clearOnSet, 0x32)

    val writeEvents = Reg(UInt(8 bits)) init(0)
    busCtrl.onWrite(0x00) {
      writeEvents := writeEvents + 1
    }
    busCtrl.read(writeEvents, 0x40)

    val readEvents = Reg(UInt(8 bits)) init(0)
    busCtrl.onRead(0x30) {
      readEvents := readEvents + 1
    }
    busCtrl.read(readEvents, 0x41)

    val delayed = Reg(UInt(8 bits)) init(0x3C)
    delayed := 0x3C
    busCtrl.multiCycleRead(SingleMapping(0x50), 2)
    busCtrl.read(delayed, 0x50)
  }

  class BusDut() extends Component {
    val io = new Bundle {
      val bus = slave(I2cSlaveBus())
    }

    val busCtrl = I2cSlaveBusSlaveFactory(
      io.bus,
      I2cSlaveBusSlaveFactoryConfig(slaveAddress = 0x42)
    )

    busCtrl.phase.simPublic()
    busCtrl.ack.pending.simPublic()
    busCtrl.ack.issued.simPublic()
    busCtrl.txByteLoaded.simPublic()
    busCtrl.txAwaitMasterAck.simPublic()
    busCtrl.rxByte.simPublic()

    val reg8 = busCtrl.createReadAndWrite(UInt(8 bits), 0x00)
    reg8.init(0x12)

    val reg16 = busCtrl.createWriteAndReadMultiWord(UInt(16 bits), 0x10)
    reg16.init(0x3456)

    val reg32 = busCtrl.createWriteAndReadMultiWord(UInt(32 bits), 0x20)
    reg32.init(U(BigInt("89ABCDEF", 16), 32 bits))

    val nextByte = Reg(UInt(8 bits)) init(0x66)
    nextByte := 0x66
    busCtrl.read(nextByte, 0x24)

    val readOnly = Reg(UInt(8 bits)) init(0x5A)
    readOnly := 0x5A
    busCtrl.read(readOnly, 0x30)

    val clearOnRead = Reg(Bits(8 bits)) init(B(0xA5, 8 bits))
    busCtrl.read(clearOnRead, 0x31)
    busCtrl.onRead(0x31) {
      clearOnRead := 0
    }

    val clearOnSet = Reg(Bits(8 bits)) init(B(0x0F, 8 bits))
    busCtrl.read(clearOnSet, 0x32)
    busCtrl.clearOnSet(clearOnSet, 0x32)

    val writeEvents = Reg(UInt(8 bits)) init(0)
    busCtrl.onWrite(0x00) {
      writeEvents := writeEvents + 1
    }
    busCtrl.read(writeEvents, 0x40)

    val readEvents = Reg(UInt(8 bits)) init(0)
    busCtrl.onRead(0x30) {
      readEvents := readEvents + 1
    }
    busCtrl.read(readEvents, 0x41)

    val delayed = Reg(UInt(8 bits)) init(0x3C)
    delayed := 0x3C
    busCtrl.multiCycleRead(SingleMapping(0x50), 2)
    busCtrl.read(delayed, 0x50)
  }
}

class I2cSlaveBusSlaveFactoryTester extends SpinalTesterCocotbBase {
  override def getName: String = "I2cSlaveBusSlaveFactoryTester"
  override def pythonTestLocation: String = "tester/src/test/python/spinal/I2CTester2/I2cSlaveBusSlaveFactoryTester"
  override def createToplevel: Component = new I2cSlaveBusSlaveFactoryTester.Dut().setDefinitionName("I2cSlaveBusSlaveFactoryTester")
}

class I2cSlaveBusSlaveFactoryElaborationTester extends SpinalAnyFunSuite {
  test("register_address_width_16") {
    SpinalConfig().generateVerilog(new I2cSlaveBusSlaveFactoryTester.Dut())
  }
}

class I2cSlaveBusSlaveFactorySimTester extends SpinalSimFunSuite {
  onlyVerilator()

  test("register_access") {
    SimConfig.withVerilator.allOptimisation.compile(new I2cSlaveBusSlaveFactoryTester.BusDut()).doSim { dut =>
      dut.clockDomain.forkStimulus(10)
      SimTimeout(200000)

      def busState = {
        s"phase=${dut.busCtrl.phase.toBigInt} ack.pending=${dut.busCtrl.ack.pending.toBoolean} " +
          s"ack.issued=${dut.busCtrl.ack.issued.toBoolean} txLoaded=${dut.busCtrl.txByteLoaded.toBoolean} " +
          s"txAwaitAck=${dut.busCtrl.txAwaitMasterAck.toBoolean} rxByte=0x${dut.busCtrl.rxByte.toBigInt.toString(16)}"
      }

      def idleCycle(): Unit = {
        dut.io.bus.cmd.kind #= I2cSlaveCmdMode.NONE
        dut.io.bus.cmd.data #= false
        dut.clockDomain.waitSampling()
      }

      def emit(kind: I2cSlaveCmdMode.E, data: Boolean = false): Unit = {
        dut.io.bus.cmd.kind #= kind
        dut.io.bus.cmd.data #= data
        dut.clockDomain.waitSampling()
      }

      def startFrame(): Unit = {
        emit(I2cSlaveCmdMode.START)
        idleCycle()
      }

      def restartFrame(): Unit = {
        emit(I2cSlaveCmdMode.RESTART)
        idleCycle()
      }

      def stopFrame(): Unit = {
        emit(I2cSlaveCmdMode.STOP)
        idleCycle()
      }

      def awaitDrive(): (Boolean, Boolean) = {
        while (true) {
          dut.io.bus.cmd.kind #= I2cSlaveCmdMode.DRIVE
          dut.io.bus.cmd.data #= false
          dut.clockDomain.waitSampling()
          if (dut.io.bus.rsp.valid.toBoolean) {
            return (dut.io.bus.rsp.enable.toBoolean, dut.io.bus.rsp.data.toBoolean)
          }
        }
        (false, false)
      }

      def writeByte(value: Int): Boolean = {
        for (bitId <- 0 until 8) {
          emit(I2cSlaveCmdMode.READ, ((value >> (7 - bitId)) & 1) != 0)
        }
        val (enable, data) = awaitDrive()
        val ack = if (enable) data else true
        emit(I2cSlaveCmdMode.READ, ack)
        ack
      }

      def readByte(masterNack: Boolean): Int = {
        var value = 0
        for (_ <- 0 until 8) {
          val (enable, data) = awaitDrive()
          val bit = if (enable) data else true
          value = (value << 1) | (if (bit) 1 else 0)
          emit(I2cSlaveCmdMode.READ, bit)
        }
        val (enable, _) = awaitDrive()
        assert(!enable, s"Master ACK slot should release SDA: $busState")
        emit(I2cSlaveCmdMode.READ, masterNack)
        value
      }

      def sendAddress(read: Boolean): Unit = {
        assert(!writeByte((0x42 << 1) | (if (read) 1 else 0)), s"Address ACK failed: $busState")
      }

      def writeBytes(address: Int, values: Seq[Int]): Unit = {
        startFrame()
        sendAddress(read = false)
        assert(!writeByte(address), s"Register pointer ACK failed at 0x${address.toHexString}: $busState")
        values.foreach { value =>
          assert(!writeByte(value), s"Payload ACK failed for 0x${value.toHexString}: $busState")
        }
        stopFrame()
      }

      def readBytes(address: Int, count: Int): Seq[Int] = {
        startFrame()
        sendAddress(read = false)
        assert(!writeByte(address), s"Read pointer ACK failed at 0x${address.toHexString}: $busState")
        restartFrame()
        sendAddress(read = true)
        val ret = for (index <- 0 until count) yield readByte(masterNack = index == count - 1)
        stopFrame()
        ret
      }

      def readCurrent(count: Int): Seq[Int] = {
        startFrame()
        sendAddress(read = true)
        val ret = for (index <- 0 until count) yield readByte(masterNack = index == count - 1)
        stopFrame()
        ret
      }

      idleCycle()
      idleCycle()

      assert(readBytes(0x00, 1) == Seq(0x12))

      writeBytes(0x00, Seq(0x33))
      assert(readBytes(0x00, 1) == Seq(0x33))
      assert(readBytes(0x40, 1) == Seq(0x01))

      assert(readBytes(0x10, 2) == Seq(0x56, 0x34))
      writeBytes(0x10, Seq(0x78, 0x56))
      assert(readBytes(0x10, 2) == Seq(0x78, 0x56))

      assert(readBytes(0x20, 4) == Seq(0xEF, 0xCD, 0xAB, 0x89))
      writeBytes(0x20, Seq(0x10, 0x20, 0x30, 0x40))
      assert(readBytes(0x20, 4) == Seq(0x10, 0x20, 0x30, 0x40))
      assert(readCurrent(1) == Seq(0x66))

      assert(readBytes(0x30, 1) == Seq(0x5A))
      assert(readBytes(0x41, 1) == Seq(0x01))
      writeBytes(0x30, Seq(0x00))
      assert(readBytes(0x30, 1) == Seq(0x5A))

      assert(readBytes(0x31, 1) == Seq(0xA5))
      assert(readBytes(0x31, 1) == Seq(0x00))

      assert(readBytes(0x32, 1) == Seq(0x0F))
      writeBytes(0x32, Seq(0x05))
      assert(readBytes(0x32, 1) == Seq(0x0A))

      assert(readBytes(0xF0, 1) == Seq(0x00))
      writeBytes(0xF0, Seq(0x99))

      assert(readBytes(0x50, 1) == Seq(0x3C))
    }
  }
}
