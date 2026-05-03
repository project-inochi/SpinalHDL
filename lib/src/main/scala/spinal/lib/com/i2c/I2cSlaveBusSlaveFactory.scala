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

  private val registerAddressByteCount = cfg.registerAddressWidth / 8
  private val registerAddressCounterWidth = log2Up(registerAddressByteCount + 1) max 1

  private val slaveAddressBits = B(cfg.slaveAddress, 7 bits)
  private val autoIncrementEnabled = cfg.autoIncrement
  private val nackOnUnmappedReadEnabled = cfg.nackOnUnmappedRead
  private val nackOnUnmappedWriteEnabled = cfg.nackOnUnmappedWrite

  val phase = Reg(I2cSlaveBusSlaveFactoryPhase()) init(ADDRESS)
  val currentPointer = Reg(UInt(cfg.registerAddressWidth bits)) init(0)
  val stagedPointer = Reg(UInt(cfg.registerAddressWidth bits)) init(0)
  val registerBytesLeft = Reg(UInt(registerAddressCounterWidth bits)) init(0)

  val rxBitCounter = Reg(UInt(3 bits)) init(0)
  val rxShift = Reg(Bits(8 bits)) init(0)
  val rxByte = Reg(Bits(8 bits)) init(0)

  val ackPending = RegInit(False)
  val ackIssued = RegInit(False)
  val ackEnable = RegInit(False)
  val ackData = RegInit(True)
  val nextPhase = Reg(I2cSlaveBusSlaveFactoryPhase()) init(ADDRESS)

  val txByte = Reg(Bits(8 bits)) init(0)
  val txByteLoaded = RegInit(False)
  val txAwaitMasterAck = RegInit(False)

  val askWriteCmd = False
  val askReadCmd = False
  val doWriteCmd = False
  val doReadCmd = False
  val hitAny = False
  val readHaltRequest = False
  val writeHaltRequest = False

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

  override def readHalt(): Unit = readHaltRequest := True
  override def writeHalt(): Unit = writeHaltRequest := True

  bus.rsp.valid := False
  bus.rsp.enable := False
  bus.rsp.data := True

  private def pointerIncremented(pointer: UInt): UInt = {
    (pointer + 1).resized
  }

  private def pointerShifted(pointer: UInt, byte: Bits): UInt = {
    (((pointer |<< 8).resized) | byte.asUInt.resized).resized
  }

  val receivedByte = (rxShift(6 downto 0) ## bus.cmd.data).asBits
  val addressMatched = rxByte(7 downto 1) === slaveAddressBits
  val addressRead = rxByte(0)
  val stagedPointerNext = pointerShifted(stagedPointer, rxByte)
  val registerByteIsLast = registerBytesLeft === 1
  val nackOnUnmappedWrite = if (nackOnUnmappedWriteEnabled) !hitAny else False
  val nackOnUnmappedRead = if (nackOnUnmappedReadEnabled) !hitAny else False

  when(bus.cmd.kind === I2cSlaveCmdMode.DRIVE) {
    switch(phase) {
      is(READ) {
        when(txAwaitMasterAck) {
          bus.rsp.valid := True
          bus.rsp.enable := False
          bus.rsp.data := True
        } elsewhen (!txByteLoaded) {
          askReadCmd := True
          when(!readHaltRequest) {
            txByte := readDataCmd
            txByteLoaded := True
          }
        } otherwise {
          bus.rsp.valid := True
          bus.rsp.enable := True
          bus.rsp.data := txByte(7 - rxBitCounter)
        }
      }
      is(WAIT_STOP) {
        bus.rsp.valid := True
        bus.rsp.enable := False
        bus.rsp.data := True
      }
      default {
        when(ackPending) {
          when(!ackIssued) {
            switch(phase) {
              is(ADDRESS) {
                when(!addressMatched) {
                  ackIssued := True
                  ackEnable := False
                  ackData := True
                  nextPhase := WAIT_STOP
                } elsewhen (addressRead) {
                  askReadCmd := True
                  when(!readHaltRequest && !nackOnUnmappedRead) {
                    txByte := readDataCmd
                    txByteLoaded := True
                    ackIssued := True
                    ackEnable := True
                    ackData := False
                    nextPhase := READ
                  } elsewhen (!readHaltRequest) {
                    ackIssued := True
                    ackEnable := False
                    ackData := True
                    nextPhase := WAIT_STOP
                  }
                } otherwise {
                  stagedPointer := 0
                  registerBytesLeft := registerAddressByteCount
                  ackIssued := True
                  ackEnable := True
                  ackData := False
                  nextPhase := REGISTER
                }
              }
              is(REGISTER) {
                stagedPointer := stagedPointerNext
                when(registerByteIsLast) {
                  currentPointer := stagedPointerNext
                  registerBytesLeft := 0
                  nextPhase := WRITE
                } otherwise {
                  registerBytesLeft := registerBytesLeft - 1
                  nextPhase := REGISTER
                }
                ackIssued := True
                ackEnable := True
                ackData := False
              }
              is(WRITE) {
                askWriteCmd := True
                when(!writeHaltRequest) {
                  ackIssued := True
                  ackEnable := !nackOnUnmappedWrite
                  ackData := False
                  when(nackOnUnmappedWrite) {
                    nextPhase := WAIT_STOP
                  } otherwise {
                    nextPhase := WRITE
                  }
                  when(!nackOnUnmappedWrite) {
                    doWriteCmd := True
                    if (autoIncrementEnabled) {
                      currentPointer := pointerIncremented(currentPointer)
                    }
                  }
                }
              }
            }
          }

          when(ackIssued) {
            bus.rsp.valid := True
            bus.rsp.enable := ackEnable
            bus.rsp.data := ackData
          }
        }
      }
    }
  }

  switch(bus.cmd.kind) {
    is(I2cSlaveCmdMode.START) {
      phase := ADDRESS
      rxBitCounter := 0
      rxShift := 0
      ackPending := False
      ackIssued := False
      txAwaitMasterAck := False
      txByteLoaded := False
      stagedPointer := 0
      registerBytesLeft := 0
    }
    is(I2cSlaveCmdMode.RESTART) {
      phase := ADDRESS
      rxBitCounter := 0
      rxShift := 0
      ackPending := False
      ackIssued := False
      txAwaitMasterAck := False
      txByteLoaded := False
      stagedPointer := 0
      registerBytesLeft := 0
    }
    is(I2cSlaveCmdMode.STOP) {
      phase := ADDRESS
      rxBitCounter := 0
      rxShift := 0
      ackPending := False
      ackIssued := False
      txAwaitMasterAck := False
      txByteLoaded := False
      stagedPointer := 0
      registerBytesLeft := 0
      if (!cfg.retainAddressPointerOnStop) {
        currentPointer := 0
      }
    }
    is(I2cSlaveCmdMode.DROP) {
      phase := ADDRESS
      rxBitCounter := 0
      rxShift := 0
      ackPending := False
      ackIssued := False
      txAwaitMasterAck := False
      txByteLoaded := False
      stagedPointer := 0
      registerBytesLeft := 0
      if (!cfg.retainAddressPointerOnStop) {
        currentPointer := 0
      }
    }
    is(I2cSlaveCmdMode.READ) {
      switch(phase) {
        is(READ) {
          when(txAwaitMasterAck) {
            doReadCmd := True
            if (autoIncrementEnabled) {
              currentPointer := pointerIncremented(currentPointer)
            }
            txAwaitMasterAck := False
            txByteLoaded := False
            rxBitCounter := 0
            when(bus.cmd.data) {
              phase := WAIT_STOP
            }
          } otherwise {
            when(txByteLoaded) {
              when(rxBitCounter === 7) {
                txAwaitMasterAck := True
                rxBitCounter := 0
              } otherwise {
                rxBitCounter := rxBitCounter + 1
              }
            }
          }
        }
        is(WAIT_STOP) {
        }
        default {
          when(ackPending) {
            when(ackIssued) {
              ackPending := False
              ackIssued := False
              rxBitCounter := 0
              phase := nextPhase
            }
          } otherwise {
            rxShift := receivedByte
            when(rxBitCounter === 7) {
              rxByte := receivedByte
              ackPending := True
              ackIssued := False
              rxBitCounter := 0
            } otherwise {
              rxBitCounter := rxBitCounter + 1
            }
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
