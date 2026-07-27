package spinal.lib.bus.tilelink

import org.scalatest.funsuite.AnyFunSuite
import spinal.core._
import spinal.core.sim._
import spinal.lib.bus.misc.SizeMapping

class IntentChannelTester extends AnyFunSuite {
  private def intentBusParameter(): BusParameter = {
    val transfers = M2sTransfers(hint = SizeRange(1, 4))
    val master = M2sAgent(
      name = null,
      mapping = List(M2sSource(
        id = SizeMapping(0, 1),
        emits = transfers
      ))
    )
    M2sParameters(
      addressWidth = 8,
      dataWidth = 32,
      masters = List(master)
    ).toNodeParameters().toBusParameter()
  }

  test("hint capability exposes the A payload required by Intent") {
    val transfers = M2sTransfers(hint = SizeRange(1, 4))
    assert(transfers.withDataA)
    assert(transfers.sizeBytes == 4)

    val p = intentBusParameter()
    val compiled = SimConfig.compile(new Component {
      val intent = ChannelA(p)
      val put = ChannelA(p)
      val intentLast = out Bool()
      val putLast = out Bool()

      assert(intent.mask != null)
      assert(intent.data != null)
      assert(intent.corrupt != null)
      assert(intent.withMask)
      assert(intent.withData)

      intent.opcode := Opcode.A.INTENT
      intentLast := intent.withBeats
      put.opcode := Opcode.A.PUT_FULL_DATA
      putLast := put.withBeats
    })

    compiled.doSim { dut =>
      assert(!dut.intentLast.toBoolean)
      assert(dut.putLast.toBoolean)
    }
  }
}
