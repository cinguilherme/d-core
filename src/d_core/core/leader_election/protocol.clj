(ns d-core.core.leader-election.protocol)

(defprotocol LeaderElectionProtocol
  (acquire! [this election-id opts]
    "Try to acquire leadership for an election. Returns map with :status in #{:acquired :busy}.")
  (renew! [this election-id token opts]
    "Renew leadership using the opaque token returned by acquire!/renew!. Returns map with :status in #{:renewed :lost}.")
  (resign! [this election-id token opts]
    "Release leadership using the opaque token returned by acquire!/renew!. Returns map with :status in #{:released :not-owner}.")
  (status [this election-id opts]
    "Return current leadership state for an election. Returns map with :status in #{:vacant :held}."))
