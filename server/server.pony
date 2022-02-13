use "collections"
use "debug"
use "random"
use "term"
use "time"

use "../client"
use "../utils"

type ServerId is USize

type Term is USize

actor Server
  var state: State = Initializing
  var id: ServerId = 0

  // Persisten state on all servers
  var currentTerm: Term = 1
  var votedFor: (ServerId | None) = None
  let log: Log ref

  // Volatile state on all servers
  var commitIndex: USize = 0
  var lastApplied: USize = 0

  // Following states are valid while it is a leader
  var nextIndex: Array[USize] = []
  var matchIndex: Array[USize] = []
  
  var cluster: Array[Server tag] val = []

  let timers: Timers
  var electionTimer: (Timer tag | None)

  // Timer for retrying AppendEntries RPC and heartbeating
  var rpcTimer: Array[(Timer tag | None)]

  var command_requests: Array[(USize, Client tag)] ref = []

  let rand: Rand = Rand


  new create() =>
    log = Log
    timers = Timers.create() // millisecond resolution
    electionTimer = None
    rpcTimer = []


  fun trace(msg: String val) =>
    // Debug.out("[" + ANSI.cyan() + "TRACE" + ANSI.reset() + "] <server " + id.string() + ">: " + msg)
    None

  fun debug(msg: String val) =>
    // Debug.out("[" + ANSI.blue() + "DEBUG" + ANSI.reset() + "] <server " + id.string() + ">: " + msg)
    None

  fun info(msg: String val) =>
    let strength =
      match state
      | let _: Leader tag => ANSI.bold()
      else "" end
    Debug.out("[" + ANSI.green() + "INFO" + ANSI.reset() + "]  " + strength + "<server " + id.string() + ">: " + msg + ANSI.reset())
    None


  be renew_election_timer() =>
    debug("renew election timer")

    // Cancel old timer
    match electionTimer
    | let timer: Timer tag => timers.cancel(timer)
    end

    // Set a new timer
    let expiration: U64 = 3_000_000_000 + (id.u64() * 200_000_000) // FIXME: pick a random timeout
    let to: Server tag = this
    let timer = TimerUtil.oneshot(expiration, {()(to) => to.start_election() })
    electionTimer = timer
    timers(consume timer)


  be renew_rpc_timer(target: ServerId) =>
    debug("renew RPC timer for " + target.string())

    // Cancel old timer
    try
      match rpcTimer(target)?
      | let timer: Timer tag => timers.cancel(timer)
      end

      // Setup a new timer
      let expiration: U64 = 1_000_000_000
      let to: Server tag = this
      let timer = TimerUtil.oneshot(expiration, {() => to.send_append_entries_rpc(target) })
      rpcTimer(target)? = timer
      timers(consume timer)
    else
      Abort("Invalid server ID: " + target.string())
    end


  be start(id': ServerId, cluster': Array[Server tag] val, leader: Server tag) =>
    id = id'
    cluster = cluster'
    if this is leader then
      state = Leader
      rpcTimer = Array[(Timer tag | None)].init(None, cluster.size())
      become_leader()
    else
      state = Follower
      renew_election_timer()
    end


  be start_election() =>
    debug("start election")
    become_candidate()


  fun ref become_follower() =>
    info("==> Follower")
    state = Follower
    timers.dispose()
    renew_election_timer()
    votedFor = None


  fun ref become_candidate() =>
    info("==> Candidate (term: " + (currentTerm + 1).string() + ")")

    let candidate = Candidate

    // Increment currentTerm
    currentTerm = currentTerm + 1

    // Vote for self
    candidate.votedBy.add(id)
    votedFor = id

    // Reset election timer
    // NOTE: election timer is not working here since electionTimer is oneshot
    //       and `become_candidate` is called on its expiration.

    // Send RequestVote RPCs to all other servers
    for dst in cluster.values() do
      if dst is this then
        continue
      end

      (let lastLogTerm, let lastLogIndex) = log.last_as_pair()

      dst.request_vote(this, currentTerm, id, lastLogIndex, lastLogTerm)
    end

    state = candidate
    timers.dispose()
    renew_election_timer()


  fun ref become_leader() =>
    state = Leader
    info("==> Leader (term: " + currentTerm.string() + ")")
    timers.dispose()
    rpcTimer = Array[(Timer tag | None)].init(None, cluster.size())

    let last_index = log.size()
    nextIndex = Array[USize].init(last_index + 1, cluster.size())
    matchIndex = Array[USize].init(0, cluster.size())

    for target in cluster.keys() do
      if target == id then
        continue
      end
      send_append_entries_rpc(target) // Send initial heartbeat
    end


  be send_append_entries_rpc(target: ServerId) =>
    trace("send_append_entries_rpc target=" + target.string())

    let dst =
      try
        cluster(target)?
      else
        Abort("Invalid server ID: " + target.string())
        return
      end
    
    // Verify current state
    match state
    | let _: Leader => None
    else
      debug("I'm not leader")
      return
    end

    // Determine which part of the entries should be sent
    var prevLogTerm: Term = 0
    var prevLogIndex: USize = 0
    let entries: Array[LogEntry val] trn = []
    try 
      let next = nextIndex(target)?
      if (0 < next) and (next <= log.size()) then
        entries.push(log(next)?) // NOTE: single entry
      end
      prevLogIndex = if next > 0 then next - 1 else 0 end
      prevLogTerm = try log(prevLogIndex)?.term else 0 end
    else
      Abort("Invalid server ID: " + target.string())
    end

    let entries_string = String
    for ent in entries.values() do
      entries_string.append(ent.string())
    end
    trace("send AppendEntries(currentTerm=" + currentTerm.string() + ", id=" + id.string() +
          ", prevLogIndex=" + prevLogIndex.string() + ", prevLogTerm=" + prevLogTerm.string() +
          ", entries=" + entries_string + ", commitIndex=" + commitIndex.string() + ")")

    // Send rpc
    dst.append_entries(
        this,
        currentTerm,
        id,
        prevLogIndex,
        prevLogTerm,
        consume entries,
        commitIndex
      )

    // Schedule next retry (or heartbeat)
    renew_rpc_timer(target)


  fun ref update_state_machine() =>
    while commitIndex > lastApplied do
      let idx = lastApplied + 1
      try
        let req = command_requests(0)?
        command_requests.remove(0, 1)

        if req._1 != idx then
          debug("index inconsistent")
        end
        req._2.commit_notify()
      end

      try
        let ent = log(idx)?
        info("apply entry: " + ent.string())
      else
        Abort("invalid idx")
      end

      lastApplied = idx
    end


  be request_command(from: Client tag, cmd: String) =>
    match state
    | let _: Leader => None
    else
      debug("I'm not leader (TODO: redirect)")
      // TODO: Redirect the request to the current leader
      return
    end

    // Create new entry
    log.append(LogEntry.create(currentTerm, cmd))
    try matchIndex(id)? = log.size() end

    command_requests.push((log.size(), from))

    // Send AppendEntries RPCs to all the other servers
    for id' in cluster.keys() do
      if id' == id then
        continue
      end
      send_append_entries_rpc(id')
    end


  be append_entries(
        from: Server tag,
        term: Term,
        leaderId: ServerId,
        prevLogIndex: USize,
        prevLogTerm: USize,
        entries: Array[LogEntry val] val,
        leaderCommit: USize
      ) =>
    // Emulate packet loss
    if (rand.next() % 10) <= 3 then return end

    let entries_string = String
    for ent in entries.values() do
      entries_string.append(ent.string())
    end
    trace("recv AppendEntries(term=" + term.string() + ", leaderId=" + leaderId.string() +
          ", prevLogIndex=" + prevLogIndex.string() + ", prevLogTerm=" + prevLogTerm.string() +
          ", entries=" + entries_string + ", leaderCommit=" + leaderCommit.string() + ")")

    // 1. Reply false if term < currentTerm
    if term < currentTerm then
      debug("RPC fail due to old term")
      from.append_entries_reply(id, currentTerm, false, prevLogIndex)
      return
    end

    currentTerm = term
    
    // Update state
    match state
    | let _: Leader if (currentTerm < term) =>
      become_follower()
    | let _: Candidate =>
      become_follower()
    | let _: Follower =>
      renew_election_timer()
    | let _: Initializing => Abort("not yet ready")
    end

    // 2. Reply false if log doesn't contain an entry
    //    at prevLogIndex whose term matches prevLogTerm
    if prevLogIndex > 0 then
      let matches: Bool = try
        let ent = log(prevLogIndex)?
        ent.term == prevLogTerm
      else
        false // no entry exist in the log at prevLogIndex
      end
      if not matches then
        debug("RPC fail due to log inconsistency")
        from.append_entries_reply(id, currentTerm, false, prevLogIndex)
        return
      end
    end

    // 3. If existing entry conflics with a new one (same index but different terms),
    //    delete the existing entry and all that follow it
    // NOTE: not actually needed to remove correct entries
    log.truncate(prevLogIndex)
    
    // 4. Append any new entries not already in the log
    // NOTE: append all entries since we used `truncate`
    for ent in entries.values() do
      log.append(ent)
    end

    // 5. If leaderCommit > commitIndex,
    //    set commitIndex = min(leaderCommit, index of last new entry)
    if leaderCommit > commitIndex then
      commitIndex = leaderCommit.min(log.size())
    end

    // Reply true
    debug("AppendEntries succeeded")
    from.append_entries_reply(id, currentTerm, true, prevLogIndex + entries.size())

    update_state_machine()


  be append_entries_reply(from: ServerId, term: Term, success: Bool, last_idx: USize) =>
    // Emulate packet loss
    if (rand.next() % 10) <= 3 then return end

    trace("append_entries_reply from " + from.string() + " (success: " + success.string() + ")m")
    if currentTerm < term then
      currentTerm = term
      become_follower()
    end

    match state
    | let _: Leader => None
    else
      debug("I'm not leader")
      return
    end

    if success then
      // If successful, update nextIndex and matchIndex for the follower
      try
        nextIndex(from)? = last_idx + 1
        if (last_idx + 1) <= log.size() then
          info("need more RPCs")
          // an entry with the index `last_idx` is correctly replicated,
          matchIndex(from)? = last_idx
          // but another AppendEntries RPC is needed
          send_append_entries_rpc(from)
        else
          info("server " + from.string() + " is up-to-date")
          // the server is up-to-date
          matchIndex(from)? = last_idx
          // Schedule next heartbeat
          renew_rpc_timer(from)
        end
      else
        Abort("Invalid server ID: " + from.string())
      end
    else
      // If AppendEntries fails because of log inconsistency,
      // decrement nextIndex and retry
      try
        debug("retry RPC for " + from.string() + ": next=" + nextIndex(from)?.string())
        nextIndex(from)? = nextIndex(from)? - 1
        send_append_entries_rpc(from)
      else
        Abort("Invalid server ID: " + from.string())
      end
    end

    // If there exists an N such that N > commitIndex,
    // a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm,
    // set commitIndex = N
    for n in Reverse(log.size(), 1) do
      try
        if log(n)?.term != currentTerm then
          break
        end
      end

      var count: USize = 0
      for idx in matchIndex.values() do
        if idx >= n then
          count = count + 1
        end
      end

      // majority of servers meet machIndex >= n
      if (count * 2) > cluster.size() then
        commitIndex = commitIndex.max(n)
        break
      end
    end

    update_state_machine()


  be request_vote(
        from: Server tag,
        term: Term,
        candidateId: ServerId,
        lastLogIndex: USize,
        lastLogTerm: USize
      ) =>
    // Emulate packet loss
    if (rand.next() % 10) <= 3 then return end

    trace("recv RequestVote(term=" + term.string() + ", candidateId=" + candidateId.string() +
          ", lastLogIndex=" + lastLogIndex.string() + ", lastLogTerm=" + lastLogTerm.string() + ")")

    if currentTerm < term then
      currentTerm = term
      become_follower()
    end

    // 1. Reply false if term < currentTerm
    if term < currentTerm then
      // NOTE: don't follow obsolete leaders
      from.request_vote_reply(id, currentTerm, false)
      return
    end

    // 2. If votedFor is null or candidateId,
    //    and candidate's log is at least as up-to-date as receiver's log,
    //    grant vote
    let up_to_date = (
        let last_ent_candidate = (lastLogTerm, lastLogIndex)
        let last_ent_receiver = log.last_as_pair()
        LogCompare.at_least_up_to_date(last_ent_candidate, last_ent_receiver)
      )
    let to_grant = ((votedFor is None) or (votedFor is candidateId)) and up_to_date
    from.request_vote_reply(id, currentTerm, to_grant)

    // Remember the vote
    if to_grant then
      votedFor = candidateId
    end

    renew_election_timer()


  be request_vote_reply(from: ServerId, term: Term, voteGranted: Bool) =>
    // Emulate packet loss
    if (rand.next() % 10) <= 3 then return end

    trace("request_vote_reply from=" + from.string() + ", voteGranded=" + voteGranted.string())

    if currentTerm < term then
      currentTerm = term
      become_follower()
    end

    match state
    | let candidate: Candidate ref if voteGranted =>
      candidate.votedBy = candidate.votedBy.add(from)

      // If receives votes from majority of servers
      if (candidate.votedBy.size() * 2) > cluster.size() then
        // become a leader
        become_leader()
      end
    end

