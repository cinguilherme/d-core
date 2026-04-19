(ns d-core.core.leader-election.redis-common
  (:require [clojure.string :as str]))

(def default-prefix
  "dcore:leader-election:")

(def ^:private shared-script-preamble
  "local call = redis and redis.call or server.call;")

(def acquire-lua
  (str shared-script-preamble
       "if call('EXISTS', KEYS[1]) == 0 then "
       "  local fencing = call('INCR', KEYS[2]);"
       "  call('HSET', KEYS[1], "
       "       'owner_id', ARGV[1], "
       "       'token', ARGV[2], "
       "       'fencing', tostring(fencing), "
       "       'acquired_at_ms', ARGV[3], "
       "       'renewed_at_ms', ARGV[3]);"
       "  call('PEXPIRE', KEYS[1], ARGV[4]);"
       "  return {'acquired', ARGV[1], tostring(fencing), ARGV[2], ARGV[4]};"
       "end;"
       "local ttl = call('PTTL', KEYS[1]);"
       "local values = call('HMGET', KEYS[1], 'owner_id', 'fencing');"
       "return {'busy', values[1], values[2], '', tostring(ttl)};"))

(def renew-lua
  (str shared-script-preamble
       "local current = call('HGET', KEYS[1], 'token');"
       "if not current then "
       "  return {'lost'};"
       "end;"
       "local ttl = call('PTTL', KEYS[1]);"
       "if current ~= ARGV[1] then "
       "  local values = call('HMGET', KEYS[1], 'owner_id', 'fencing');"
       "  return {'lost', values[1], values[2], tostring(ttl)};"
       "end;"
       "call('HSET', KEYS[1], 'renewed_at_ms', ARGV[2]);"
       "call('PEXPIRE', KEYS[1], ARGV[3]);"
       "local values = call('HMGET', KEYS[1], 'owner_id', 'fencing');"
       "return {'renewed', values[1], values[2], ARGV[1], ARGV[3]};"))

(def resign-lua
  (str shared-script-preamble
       "local current = call('HGET', KEYS[1], 'token');"
       "if not current then "
       "  return {'not-owner'};"
       "end;"
       "local ttl = call('PTTL', KEYS[1]);"
       "local values = call('HMGET', KEYS[1], 'owner_id', 'fencing');"
       "if current ~= ARGV[1] then "
       "  return {'not-owner', values[1], values[2], tostring(ttl)};"
       "end;"
       "call('DEL', KEYS[1]);"
       "return {'released', values[1], values[2]};"))

(def status-lua
  (str shared-script-preamble
       "if call('EXISTS', KEYS[1]) == 0 then "
       "  return {'vacant'};"
       "end;"
       "local ttl = call('PTTL', KEYS[1]);"
       "local values = call('HMGET', KEYS[1], 'owner_id', 'fencing');"
       "return {'held', values[1], values[2], tostring(ttl)};"))

(defn normalize-prefix
  [prefix]
  (let [value (str (or prefix default-prefix))]
    (when (str/blank? value)
      (throw (ex-info "Leader election prefix must not be blank"
                      {:type ::invalid-prefix
                       :prefix prefix})))
    value))

(defn lease-key
  [prefix election-id]
  (str prefix election-id ":lease"))

(defn fencing-key
  [prefix election-id]
  (str prefix election-id ":fencing"))
