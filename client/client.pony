use "debug"
use "term"

use "../server"

actor Client
  let server: Server tag

  new create(server': Server tag) =>
    server = server'

  be request_command(cmd: String val) =>
    server.request_command(this, cmd)

  be commit_notify() =>
    Debug.out(ANSI.yellow() + "command applied" + ANSI.reset())
