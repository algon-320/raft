class val LogEntry
  let term: Term
  let command: String val

  new val create(term': Term, command': String val) =>
    term = term'
    command = command'

  fun string(): String iso^ =>
    let buf = recover iso String end
    buf.append("(term: ")
    buf.append(term.string())
    buf.append(", command: ")
    buf.append(command)
    buf.append(")")
    consume buf

class Log
  var _entries: Array[LogEntry] ref = []

  fun ref append(ent: LogEntry) =>
    _entries.push(ent)

  fun size(): USize val => _entries.size()

  // 1-origin
  fun apply(at: USize): LogEntry val? =>
    if at == 0 then
      error
    else
      _entries(at - 1)?
    end

  // Returns the last entry as a pair (term, index)
  fun last_as_pair(): (Term, USize) =>
    try
      let len = _entries.size()
      let ent = _entries(len - 1)?
      (ent.term, len - 1)
    else
      (0, 0)
    end

  fun ref truncate(len: USize) =>
    _entries.truncate(len)

  fun string(): String iso^ =>
    let ret = recover iso String end
    for ent in _entries.values() do
      ret.append(ent.string())
      ret.append(", ")
    end
    consume ret

primitive LogCompare
  fun at_least_up_to_date(
        last_ent_a: (Term, USize),
        last_ent_b: (Term, USize)
      ): Bool =>
    if last_ent_a._1 == last_ent_b._1 then
      last_ent_a._2 >= last_ent_b._2
    else
      last_ent_a._1 >= last_ent_b._1
    end

    
