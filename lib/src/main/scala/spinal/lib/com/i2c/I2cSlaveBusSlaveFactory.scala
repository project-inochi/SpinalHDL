package spinal.lib.com.i2c

import spinal.core._
import spinal.lib._
import spinal.lib.bus.misc._

case class I2cSlaveBusSlaveFactoryConfig(
    slaveAddress: Int,
    registerAddressWidth: Int = 8,
    autoIncrement: Boolean = true,
    retainAddressPointerOnStop: Boolean = true,
    nackOnUnmappedRead: Boolean = false,
    nackOnUnmappedWrite: Boolean = false
)

object I2cSlaveBusSlaveFactory {
  def apply(bus: I2cSlaveBus, cfg: I2cSlaveBusSlaveFactoryConfig): I2cSlaveBusSlaveFactory = {
    new I2cSlaveBusSlaveFactory(bus, cfg)
  }
}

object I2cSlaveBusSlaveFactoryPhase extends SpinalEnum {
  val ADDRESS, REGISTER, WRITE, READ, WAIT_STOP = newElement()
}

class I2cSlaveBusSlaveFactory(bus: I2cSlaveBus, cfg: I2cSlaveBusSlaveFactoryConfig) extends BusSlaveFactoryDelayed {
  import I2cSlaveBusSlaveFactoryPhase._

  require(cfg.slaveAddress >= 0 && cfg.slaveAddress < 128, s"Invalid 7-bit I2C slave address ${cfg.slaveAddress}")
  require(cfg.registerAddressWidth >= 8 && cfg.registerAddressWidth % 8 == 0, "registerAddressWidth must be a multiple of 8 bits")

  private val slaveAddressBits = B(cfg.slaveAddress, 7 bits)
  private val autoIncrementEnabled = cfg.autoIncrement
  private val nackOnUnmappedReadEnabled = cfg.nackOnUnmappedRead
  private val nackOnUnmappedWriteEnabled = cfg.nackOnUnmappedWrite

  val phase = Reg(I2cSlaveBusSlaveFactoryPhase()) init(ADDRESS)
  val currentPointer = Reg(UInt(cfg.registerAddressWidth bits)) init(0)
  val stagedPointer = Reg(UInt(cfg.registerAddressWidth bits)) init(0)

  val bitCounter = new Area {
    val value = Reg(U(0, 3 bit))
    val loaded = Reg(B(0, 8 bit))
    def last = value.andR

    def reset() = value := 0

    def count() = {
      loaded(value) := bus.cmd.data
      when(last) {
        reset()
      } otherwise {
        value := value + 1
      }
    }
  }

  val rxByte = bitCounter.loaded.reversed

  val ack = new Area {
    val pending = RegInit(False)
    val issued = RegInit(False)
    val enable = RegInit(False)
    val data = RegInit(True)
  }

  val nextPhase = Reg(I2cSlaveBusSlaveFactoryPhase()) init(ADDRESS)

  val txByte = Reg(Bits(8 bits)) init(0)
  val txByteLoaded = RegInit(False)
  val txAwaitMasterAck = RegInit(False)

  val askWriteCmd = False
  val askReadCmd = False
  val doWriteCmd = False
  val doReadCmd = False
  val hitAny = False

  val writeDataCmd = Bits(8 bits)
  val readDataCmd = Bits(8 bits)
  writeDataCmd := rxByte
  readDataCmd := 0

  override def busDataWidth: Int = 8
  override def wordAddressInc: Int = 1

  override def readAddress(): UInt = currentPointer
  override def writeAddress(): UInt = currentPointer
  override def readFire(): Bool = doReadCmd
  override def writeFire(): Bool = doWriteCmd

  override def readHalt(): Unit = {}
  override def writeHalt(): Unit = {}

  bus.rsp.valid := False
  bus.rsp.enable := False
  bus.rsp.data := True

  private def pointerIncremented(pointer: UInt): UInt = {
    (pointer + 1).resized
  }

  private def pointerShifted(pointer: UInt, byte: Bits): UInt = {
    (((pointer |<< 8).resized) | byte.asUInt.resized).resized
  }

  val addressMatched = rxByte(7 downto 1) === slaveAddressBits
  val addressRead = rxByte(0)
  val nackOnUnmappedWrite = if (nackOnUnmappedWriteEnabled) !hitAny else False
  val nackOnUnmappedRead = if (nackOnUnmappedReadEnabled) !hitAny else False
  val frameStart = bus.cmd.kind === I2cSlaveCmdMode.START || bus.cmd.kind === I2cSlaveCmdMode.RESTART
  val frameStop = bus.cmd.kind === I2cSlaveCmdMode.STOP || bus.cmd.kind === I2cSlaveCmdMode.DROP

  when(bus.cmd.kind === I2cSlaveCmdMode.DRIVE) {
    switch(phase) {
      is(READ) {
        when(txAwaitMasterAck) {
          bus.rsp.valid := True
          bus.rsp.enable := False
          bus.rsp.data := True
        } elsewhen (!txByteLoaded) {
          askReadCmd := True
          txByte := readDataCmd
          txByteLoaded := True
        } otherwise {
          bus.rsp.valid := True
          bus.rsp.enable := True
          bus.rsp.data := txByte(7 - bitCounter.value)
        }
      }
      is(WAIT_STOP) {
        bus.rsp.valid := True
        bus.rsp.enable := False
        bus.rsp.data := True
      }
      default {
        when(ack.pending) {
          when(!ack.issued) {
            switch(phase) {
              is(ADDRESS) {
                when(!addressMatched) {
                  ack.issued := True
                  ack.enable := False
                  ack.data := True
                  nextPhase := WAIT_STOP
                } elsewhen (addressRead) {
                  askReadCmd := True
                  when(!nackOnUnmappedRead) {
                    txByte := readDataCmd
                    txByteLoaded := True
                    ack.issued := True
                    ack.enable := True
                    ack.data := False
                    nextPhase := READ
                  } otherwise {
                    ack.issued := True
                    ack.enable := False
                    ack.data := True
                    nextPhase := WAIT_STOP
                  }
                } otherwise {
                  ack.issued := True
                  ack.enable := True
                  ack.data := False
                  nextPhase := REGISTER
                }
              }
              is(REGISTER) {
                currentPointer := rxByte
                nextPhase := WRITE
                ack.issued := True
                ack.enable := True
                ack.data := False
              }
              is(WRITE) {
                askWriteCmd := True
                ack.issued := True
                ack.enable := !nackOnUnmappedWrite
                ack.data := False
                when(nackOnUnmappedWrite) {
                  nextPhase := WAIT_STOP
                } otherwise {
                  nextPhase := WRITE
                }
                when(!nackOnUnmappedWrite) {
                  doWriteCmd := True
                  if (autoIncrementEnabled) {
                    currentPointer := currentPointer + 1
                  }
                }
              }
            }
          }

          when(ack.issued) {
            bus.rsp.valid := True
            bus.rsp.enable := ack.enable
            bus.rsp.data := ack.data
          }
        }
      }
    }
  }

  when (frameStart || frameStop) {
    phase := ADDRESS

    ack.pending := False
    ack.issued := False
    ack.enable := False
    ack.data := True
    nextPhase := ADDRESS
    txAwaitMasterAck := False
    txByteLoaded := False

    bitCounter.reset()
  }


  if (!cfg.retainAddressPointerOnStop) when (frameStop) {
    currentPointer := 0
  }

  when (bus.cmd.kind === I2cSlaveCmdMode.READ) {
    switch(phase) {
      is(READ) {
        when(txAwaitMasterAck) {
          doReadCmd := True
          if (autoIncrementEnabled) {
            currentPointer := currentPointer + 1
          }
          txAwaitMasterAck := False
          txByteLoaded := False
          bitCounter.reset()
          when(bus.cmd.data) {
            phase := WAIT_STOP
          }
        } otherwise {
          when(txByteLoaded) {
            bitCounter.count()
            txAwaitMasterAck.setWhen(bitCounter.last)
          }
        }
      }
      is(WAIT_STOP) {
      }
      default {
        when(ack.pending) {
          when(ack.issued) {
            ack.pending := False
            ack.issued := False
            bitCounter.reset()
            phase := nextPhase
          }
        } otherwise {
          bitCounter.count()
          when(bitCounter.last) {
            ack.pending := True
            ack.issued := False
          }
        }
      }
    }
  }

  override def build(): Unit = {
    super.doNonStopWrite(writeDataCmd)

    def doMappedElements(jobs: Seq[BusSlaveFactoryElement]): Unit = {
      hitAny := True
      super.doMappedElements(
        jobs = jobs,
        askWrite = askWriteCmd,
        askRead = askReadCmd,
        doWrite = doWriteCmd,
        doRead = doReadCmd,
        writeData = writeDataCmd,
        readData = readDataCmd
      )
    }

    switch(currentPointer) {
      for ((address, jobs) <- elementsPerAddress if address.isInstanceOf[SingleMapping]) {
        is(address.asInstanceOf[SingleMapping].address) {
          doMappedElements(jobs)
        }
      }
    }

    for ((address, jobs) <- elementsPerAddress if !address.isInstanceOf[SingleMapping]) {
      when(address.hit(currentPointer)) {
        doMappedElements(jobs)
      }
    }
  }
}
