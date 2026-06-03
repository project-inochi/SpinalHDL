package spinal.tester.scalatest

import spinal.core._
import spinal.core.sim._
import spinal.lib.bus.tilelink
import spinal.lib.misc.aia._
import spinal.lib.sim._
import spinal.sim._

import scala.collection.mutable
import scala.util.Random

case class ImsicFileRamModel(p: ImsicFileParameters) {
  import p._
  val lineNum = sourceNum / xlen

  val ip = Array.fill(lineNum)(BigInt(0))
  val ie = Array.fill(lineNum)(BigInt(0))

  def write(ip: Boolean, address: Int, data: Int, mask: Int): Unit = {
    val realMask = BigInt(mask) << address
  }

  def internalMask(address: Int): BigInt = {
    require(address >= 0 && address < lineNum)

    val mask = (BigInt(1) << xlen) - 1

    if (address == 0) mask.clearBit(0) else mask
  }

  def maskedUpdate(old: BigInt, address: Int, value: BigInt, mask: BigInt): BigInt = {
    val valid = internalMask(address)
    val realMask = mask & valid
    val data = value & mask
    ((old & (valid ^ realMask)) | data) & valid
  }

  def writeIp(address: Int, data: BigInt, mask: BigInt) = {
    require(address >= 0 && address < lineNum)
    ip(address) = maskedUpdate(ip(address), address, data, mask)
  }

  def writeIe(address: Int, data: BigInt, mask: BigInt) = {
    require(address >= 0 && address < lineNum)
    ie(address) = maskedUpdate(ie(address), address, data, mask)
  }

  def readIp(address: Int): BigInt = {
    require(address >= 0 && address < lineNum)
    ip(address) & internalMask(address)
  }

  def readIe(address: Int): BigInt = {
    require(address >= 0 && address < lineNum)
    ie(address) & internalMask(address)
  }

  def trigger(id: Int) = {
    if (id > 0 && id < sourceNum) {
      writeIp(id / xlen, BigInt(0).setBit(id % xlen), BigInt(0).setBit(id % xlen))
    }
  }

  def query(): Option[Int] = (1 until sourceNum).find { id =>
    val address = id / xlen
    val mask = BigInt(0).setBit(id % xlen)
    (ip(address) & ie(address) & mask) != 0
  }
}

case class ImsicTestCmd(
  op      : SpinalEnumElement[ImsicOp.type],
  isIp    : Boolean,
  address : Int,
  data    : BigInt,
  mask    : BigInt,
)

case class ImsicRsp(
  label: String,
  snapshot: BigInt,
  data: BigInt
)

class ImsicFileCmdTester(logic: ImsicFileRamLogic, portNum: Int, model: ImsicFileRamModel) {
  case class rspContainer(
    label: String,
    address: Int,
    oldData: BigInt,
    writeConfiguration: Option[Boolean],
  )

  val cd = logic.clockDomain
  val port = logic.io.port(portNum)

  val cmdQueue = mutable.Queue[ImsicTestCmd]()
  val rspQueue = mutable.Queue[rspContainer]()
  val scoreboard = ScoreboardInOrder[ImsicRsp]()
  var testNumber = 0
  var current = 0

  def addCmd(cmd: ImsicTestCmd) = {
    cmdQueue.enqueue(cmd)
    testNumber += 1
  }

  def doIt() = {
    val portCmd = StreamDriver(port.cmd, cd) { payload =>
      if (cmdQueue.isEmpty) {
        false
      } else {
        val cmd = cmdQueue.dequeue()
        payload.op      #= cmd.op
        payload.doIp    #= cmd.isIp
        payload.address #= cmd.address
        payload.data    #= cmd.data
        payload.mask    #= cmd.mask
        true
      }
    }
    portCmd.transactionDelay = () => 0

    val portCmdMonitor = StreamMonitor(port.cmd, cd) { payload =>
      val op = payload.op.toEnum
      val isIp = payload.doIp.toBoolean
      val address = payload.address.toInt
      val data = payload.data.toBigInt
      val mask = payload.mask.toBigInt

      op match {
        case ImsicOp.READ => {
          val label = s"Read ${if (isIp) "IP" else "IE"} $address"
          val iepSnapshot = if(isIp) model.ip else model.ie
          val expected = if (isIp) model.readIp(address) else model.readIe(address)
          rspQueue.enqueue(rspContainer(
            label = label,
            address = address,
            oldData = expected,
            writeConfiguration = None,
          ))
          scoreboard.pushRef(ImsicRsp(label, expected, expected))
        }

        case ImsicOp.WRITE => {
          val label = s"Write ${if (isIp) "IP" else "IE"} $address: mask ${mask}, data ${data}"
          val iepSnapshot = if(isIp) model.ip else model.ie
          val oldData = BigInt(iepSnapshot(address).bigInteger)
          if (isIp) model.writeIp(address, data, mask) else model.writeIe(address, data, mask)
          val newData = BigInt(iepSnapshot(address).bigInteger)
          rspQueue.enqueue(rspContainer(
            label = label,
            address = address,
            oldData = oldData,
            writeConfiguration = Some(isIp),
          ))
          scoreboard.pushRef(ImsicRsp(label, oldData, newData))
        }
      }
    }

    val portRsp = FlowMonitor(port.rsp, cd) { payload =>
      assert(rspQueue.nonEmpty, "Unexpected ImsicFileRam response")
      val rsp = rspQueue.dequeue()
      val data = rsp.writeConfiguration match {
        case Some(isIp) => {
          val mem = if(isIp) logic.ip else logic.ie
          mem.getBigInt(rsp.address)
        }
        case None => payload.data.toBigInt
      }
      scoreboard.pushDut(ImsicRsp(rsp.label, rsp.oldData, data))
      current += 1
    }

    cd.waitSamplingWhere(cmdQueue.isEmpty && testNumber == current)
    scoreboard.checkEmptiness()
    cd.waitSampling()

    println(scoreboard.matches)
  }
}

class SpinalSimImsicFileTester extends SpinalSimFunSuite {
  onlyVerilator()
  val sourceNum = 512
  val xlen = 64

  def fileParameter(portNum: Int) = ImsicFileParameters(
    hartId = 0,
    guestId = 0,
    sourceNum = sourceNum,
    xlen = xlen,
    portNum = portNum
  )

  def doSim(portNum: Int)(body: (ImsicFileRamLogic, ImsicFileRamModel) => Unit) = SimConfig.withFstWave.compile {
    val logic = new ImsicFileRamLogic(fileParameter(portNum))
    logic.ie.simPublic()
    logic.ip.simPublic()
    logic
  }.doSim { dut =>
    for(id <- 0 until dut.p.portNum) {
      dut.io.port(id).cmd.valid #= false
    }
    val model = new ImsicFileRamModel(dut.p)

    dut.clockDomain.forkStimulus(10)
    dut.clockDomain.waitSampling(5)
    body(dut, model)
    dut.clockDomain.waitSampling(5)
  }

  def writeSingleBitTestCmd(id: Int, isIp: Boolean, unset: Boolean = false) = ImsicTestCmd(
    op = ImsicOp.WRITE,
    isIp = isIp,
    address = id / xlen,
    data = if (unset) 0 else BigInt(1) << (id % xlen),
    mask = BigInt(1) << (id % xlen),
  )

  def readSingleBitTestCmd(id: Int, isIp: Boolean) = ImsicTestCmd(
    op = ImsicOp.WRITE,
    isIp = isIp,
    address = id / xlen,
    data = 0,
    mask = 0,
  )

  test("simple") {
    doSim(1) { (dut, model) =>
      val tester = new ImsicFileCmdTester(dut, 0, model)

      assert(dut.io.identity.toBigInt == 0, s"Wrong identity: ${dut.io.identity.toBigInt}")

      for (id <- 1 until sourceNum) {
        tester.addCmd(writeSingleBitTestCmd(id, false, false))
        tester.addCmd(writeSingleBitTestCmd(id, true, false))
        tester.addCmd(readSingleBitTestCmd(id, false))
        tester.addCmd(readSingleBitTestCmd(id, true))
        tester.addCmd(writeSingleBitTestCmd(id, false, true))
        tester.addCmd(writeSingleBitTestCmd(id, true, true))
      }
      tester.doIt()

      assert(dut.io.identity.toBigInt == 0, s"Wrong identity: ${dut.io.identity.toBigInt}")
    }
  }
}
