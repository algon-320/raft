use "signals"
use "debug"
use "term"

primitive Abort
  fun apply(msg: String) =>
    Debug.out(ANSI.bright_red() + "ABORT: " + msg + ANSI.reset())
    SignalRaise(Sig.abrt())
