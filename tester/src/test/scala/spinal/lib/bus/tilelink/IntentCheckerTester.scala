package spinal.lib.bus.tilelink

import org.scalatest.funsuite.AnyFunSuite
import spinal.lib.bus.misc.SizeMapping
import spinal.lib.bus.tilelink.sim.{Checker, Chunk, Endpoint, IdCallback, OrderingArgs, TransactionA, TransactionD}
import spinal.lib.sim.SparseMemory

class IntentCheckerTester extends AnyFunSuite {
  private val transfers = M2sTransfers(
    get = SizeRange(1, 4),
    hint = SizeRange(1, 64)
  )
  private val p = M2sParameters(
    addressWidth = 8,
    dataWidth = 32,
    masters = List(M2sAgent(
      name = null,
      mapping = List(M2sSource(SizeMapping(0, 4), transfers))
    ))
  ).toNodeParameters().toBusParameter()

  private def newChecker(memory: SparseMemory = null, callback: IdCallback = null): Checker = {
    if(memory == null) {
      new Checker(p, Nil, checkMapping = false)(null)
    } else {
      assert(callback != null)
      new Checker(
        p,
        Seq(Endpoint(memory, Seq(Chunk(transfers, SizeMapping(0, 256), 0)))),
        checkMapping = true
      )(callback)
    }
  }

  private def intent(param: Int,
                     source: Int = 0,
                     address: BigInt = 0,
                     size: Int = 2,
                     mask: Array[Boolean] = Array.fill(4)(true),
                     corrupt: Boolean = false): TransactionA = {
    val a = TransactionA()
    a.opcode = Opcode.A.INTENT
    a.param = param
    a.source = source
    a.address = address
    a.size = size
    a.mask = mask
    a.corrupt = corrupt
    a
  }

  private def hintAck(source: Int = 0,
                      address: BigInt = 0,
                      size: Int = 2,
                      param: Int = 0,
                      corrupt: Boolean = false): TransactionD = {
    val d = new TransactionD
    d.opcode = Opcode.D.HINT_ACK
    d.source = source
    d.address = address
    d.size = size
    d.param = param
    d.corrupt = corrupt
    d
  }

  test("Checker accepts all standard Intent params and HintAck denied values") {
    val memory = SparseMemory(seed = 42)
    memory.write(0, Array[Byte](1, 2, 3, 4), Array.fill(4)(true))
    val before = memory.readBytes(0, 4)

    for((param, index) <- Seq(
      Param.Intent.PREFETCH_READ,
      Param.Intent.PREFETCH_WRITE,
      Param.Intent.CBO_INVAL,
      Param.Intent.CBO_CLEAN,
      Param.Intent.CBO_FLUSH
    ).zipWithIndex) {
      val callback = new IdCallback
      val checker = newChecker(memory, callback)
      val a = intent(param, address = index * 4)
      checker.onA(a)
      val d = hintAck(address = index * 4)
      d.denied = index % 2 == 1
      if(!d.denied) {
        callback.call(a.debugId)(new OrderingArgs(0, a.bytes))
      }
      checker.onD(d)
      assert(checker.isEmpty())
      assert(callback.tasks.isEmpty)
    }

    assert(memory.readBytes(0, 4).sameElements(before))
  }

  test("Checker accepts legal sub-beat and over-beat Intent masks") {
    val subBeat = newChecker()
    subBeat.onA(intent(
      Param.Intent.PREFETCH_READ,
      address = 2,
      size = 1,
      mask = Array(false, false, true, true)
    ))
    subBeat.onD(hintAck(address = 2, size = 1))
    assert(subBeat.isEmpty())

    val overBeat = newChecker()
    overBeat.onA(intent(
      Param.Intent.CBO_CLEAN,
      size = 3,
      mask = Array.fill(p.dataBytes)(true)
    ))
    overBeat.onD(hintAck(size = 3))
    assert(overBeat.isEmpty())

    val blockSized = newChecker()
    blockSized.onA(intent(
      Param.Intent.CBO_FLUSH,
      size = 6,
      mask = Array.fill(p.dataBytes)(true)
    ))
    blockSized.onD(hintAck(size = 6))
    assert(blockSized.isEmpty())
  }

  test("Checker rejects illegal or malformed Intent A transactions") {
    for(param <- Seq(2, 3, 4)) {
      val checker = newChecker()
      intercept[AssertionError] {
        checker.onA(intent(param))
      }
    }

    val malformed = Seq(
      intent(Param.Intent.CBO_INVAL, address = 1),
      intent(Param.Intent.CBO_INVAL, size = 7, mask = Array.fill(4)(true)),
      intent(Param.Intent.CBO_INVAL, mask = Array(true, true, false, false)),
      intent(Param.Intent.CBO_INVAL, size = 6, mask = Array(true, true, false, true)),
      intent(Param.Intent.CBO_INVAL, size = 6, mask = Array.fill(64)(true)),
      intent(Param.Intent.CBO_INVAL, corrupt = true)
    )
    for(a <- malformed) {
      val checker = newChecker()
      intercept[AssertionError] {
        checker.onA(a)
      }
    }
  }

  test("Checker rejects malformed or unmatched HintAck responses") {
    val badResponses = Seq(
      hintAck(param = 1),
      hintAck(source = 1),
      hintAck(size = 1),
      hintAck(address = 4),
      hintAck(corrupt = true)
    )
    for(d <- badResponses) {
      val checker = newChecker()
      checker.onA(intent(Param.Intent.CBO_FLUSH))
      intercept[AssertionError] {
        checker.onD(d)
      }
    }

    val checker = newChecker()
    intercept[AssertionError] {
      checker.onD(hintAck())
    }
  }
}
