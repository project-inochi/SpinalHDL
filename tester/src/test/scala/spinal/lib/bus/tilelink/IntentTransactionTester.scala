package spinal.lib.bus.tilelink

import org.scalatest.funsuite.AnyFunSuite
import spinal.core._
import spinal.core.sim._
import spinal.lib._
import spinal.lib.bus.tilelink.sim.{TransactionA, TransactionD}

class IntentTransactionTester extends AnyFunSuite {
  test("Intent and HintAck serialize as single-beat non-data transactions") {
    val p = BusParameter.simple(
      addressWidth = 8,
      dataWidth = 32,
      sizeBytes = 64,
      sourceWidth = 2
    )
    val a = TransactionA()
    a.opcode = Opcode.A.INTENT
    a.param = Param.Intent.CBO_CLEAN
    a.source = 1
    a.address = 0x40
    a.size = 6
    a.mask = Array.fill(64)(true)
    a.data = null
    a.corrupt = false

    val aBeats = a.serialize(p.dataBytes)
    assert(a.withMask)
    assert(!a.withData)
    assert(aBeats.length == 1)
    assert(aBeats.head.size == 6)
    assert(aBeats.head.mask.length == p.dataBytes)
    assert(aBeats.head.mask.forall(identity))
    assert(aBeats.head.data == null)

    val d = TransactionD(a)
    d.opcode = Opcode.D.HINT_ACK
    d.param = 0
    d.denied = true
    d.corrupt = false

    val dBeats = d.serialize(p.dataBytes)
    assert(!d.withData)
    assert(dBeats.length == 1)
    assert(dBeats.head.size == 6)
    assert(dBeats.head.data == null)
  }

  test("Intent matches only a HintAck with the same source, address and size") {
    val a = TransactionA()
    a.opcode = Opcode.A.INTENT
    a.source = 2
    a.address = 0x40
    a.size = 2
    a.mask = Array(true, true, true, true)

    val d = TransactionD(a)
    d.opcode = Opcode.D.HINT_ACK
    assert(d.isRspOf(a))

    val wrongSource = TransactionD(a)
    wrongSource.opcode = Opcode.D.HINT_ACK
    wrongSource.source = 3
    assert(!wrongSource.isRspOf(a))

    val wrongSize = TransactionD(a)
    wrongSize.opcode = Opcode.D.HINT_ACK
    wrongSize.size = 1
    assert(!wrongSize.isRspOf(a))

    val wrongOpcode = TransactionD(a)
    wrongOpcode.opcode = Opcode.D.ACCESS_ACK
    assert(!wrongOpcode.isRspOf(a))
  }

  test("Intent and HintAck physical payloads round-trip through channels") {
    val p = BusParameter.simple(
      addressWidth = 8,
      dataWidth = 32,
      sizeBytes = 4,
      sourceWidth = 2
    )
    val a = TransactionA()
    a.opcode = Opcode.A.INTENT
    a.param = Param.Intent.PREFETCH_READ
    a.source = 1
    a.address = 0x12
    a.size = 1
    a.mask = Array(true, true)
    a.corrupt = false
    a.debugId = 0

    val d = TransactionD(a)
    d.opcode = Opcode.D.HINT_ACK
    d.param = 0
    d.denied = true
    d.corrupt = false

    val compiled = SimConfig.compile(new Component {
      val aChannel = slave Stream(ChannelA(p))
      val dChannel = slave Stream(ChannelD(p))
      aChannel.ready := True
      dChannel.ready := True
    })

    compiled.doSim { dut =>
      dut.clockDomain.forkStimulus(10)
      a.serialize(p.dataBytes).head.write(dut.aChannel.payload)
      d.serialize(p.dataBytes).head.write(dut.dChannel.payload)
      dut.clockDomain.waitSampling()

      val readA = TransactionA(dut.aChannel.payload)
      val readD = TransactionD(dut.dChannel.payload, a.address)
      assert(readA.opcode == Opcode.A.INTENT)
      assert(readA.withMask)
      assert(!readA.withData)
      assert(readA.mask sameElements Array(false, false, true, true))
      assert(readA.data == null)
      assert(!readA.corrupt)
      assert(readD.opcode == Opcode.D.HINT_ACK)
      assert(!readD.withData)
      assert(readD.denied)
      assert(!readD.corrupt)
      assert(readD.isRspOf(readA))
    }
  }
}
