use "collections"

type State is (Initializing | Leader | Candidate | Follower)
class Initializing
class Leader
class Candidate
  var votedBy: Set[ServerId] ref
  new create() => 
    votedBy = Set[ServerId].create()
class Follower
