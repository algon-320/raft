use "time"
use "debug"

primitive TimerUtil
  fun interval(interval_ns: U64, callback: {()} val): Timer iso^ =>
    let notify = recover iso
        object is TimerNotify
          fun ref apply(timer: Timer ref, count: U64): Bool =>
            callback()
            true
          fun ref cancel(timer: Timer ref) =>
            None
        end
      end
    Timer.create(consume notify, interval_ns, interval_ns)

  fun oneshot(expiration_ns: U64, callback: {()} val): Timer iso^ =>
    let notify = recover iso
        object is TimerNotify
          fun ref apply(timer: Timer ref, count: U64): Bool =>
            callback()
            false
          fun ref cancel(timer: Timer ref) =>
            None
        end
      end
    Timer.create(consume notify, expiration_ns, 0)
