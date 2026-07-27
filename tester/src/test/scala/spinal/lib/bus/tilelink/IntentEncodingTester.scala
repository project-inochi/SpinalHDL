package spinal.lib.bus.tilelink

import org.scalatest.funsuite.AnyFunSuite
import spinal.core._
import spinal.core.sim._

class IntentEncodingTester extends AnyFunSuite {
  test("Intent and HintAck use the standard opcode encodings") {
    assert(Opcode.A.defaultEncoding.getValue(Opcode.A.PUT_FULL_DATA) == 0)
    assert(Opcode.A.defaultEncoding.getValue(Opcode.A.INTENT) == 5)
    assert(Opcode.D.defaultEncoding.getValue(Opcode.D.ACCESS_ACK) == 0)
    assert(Opcode.D.defaultEncoding.getValue(Opcode.D.HINT_ACK) == 2)
  }

  test("Intent parameters are independent from the private Hint parameter") {
    assert(Param.Intent.PREFETCH_READ == 0)
    assert(Param.Intent.PREFETCH_WRITE == 1)
    assert(Param.Intent.CBO_INVAL == 5)
    assert(Param.Intent.CBO_CLEAN == 6)
    assert(Param.Intent.CBO_FLUSH == 7)
    assert(Param.Hint.NO_ALLOCATE_ON_MISS == 2)
    assert(Param.Intent.PREFETCH_WRITE != Param.Hint.NO_ALLOCATE_ON_MISS)
  }

  test("M2sTransfers uses hint capability for Intent") {
    val withIntent = M2sTransfers(hint = SizeRange(64))
    val withoutIntent = M2sTransfers()

    assert(withIntent.allowA(Opcode.A.INTENT))
    assert(withIntent.allow(Opcode.A.INTENT))
    assert(!withoutIntent.allowA(Opcode.A.INTENT))
    assert(!withoutIntent.allow(Opcode.A.INTENT))
  }

  test("HintAck is an A response and a final non-data D response") {
    val compiled = SimConfig.compile(new Component {
      val fromA = out Bool()
      val isFinal = out Bool()
      val isData = out Bool()
      fromA := Opcode.D.fromA(Opcode.D.HINT_ACK)
      isFinal := Opcode.D.isFinal(Opcode.D.HINT_ACK)
      isData := Opcode.D.isData(Opcode.D.HINT_ACK)
    })

    compiled.doSim { dut =>
      assert(dut.fromA.toBoolean)
      assert(dut.isFinal.toBoolean)
      assert(!dut.isData.toBoolean)
    }
  }
}
