use "collections"

use "client"
use "server"
use "utils"

actor Main
  let _env: Env

  new create(env: Env) =>
    _env = env
    start_raft()

  fun start_raft() =>
    // Launch servers
    let cluster =
      recover val 
        let c = Array[Server tag]
        for i in Range(0, 5) do
          c.push(Server)
        end
        c
      end

    // Start servers
    let initial_leader =
      try
        cluster(0)?
      else
        Abort("no server")
        return
      end
    for (id, s) in cluster.pairs() do
      s.start(id, cluster, initial_leader) end

    let client = Client(initial_leader)

    // Issue some commands
    client.request_command("foo")
    client.request_command("bar")
    client.request_command("baz")

    run()

  be run() => run()

