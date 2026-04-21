(ns d-core.core.leader-election.logics.kubernetes-lease
  (:require [d-core.core.leader-election.common :as common]))

(defn- holder-response
  [status lease now-ms remaining-ttl-ms]
  [(name status)
   (:holder-identity lease)
   (some-> (:lease-transitions lease) str)
   (some-> (remaining-ttl-ms lease now-ms) str)])

(defn- acquired-response
  [lease token now-ms remaining-ttl-ms]
  ["acquired"
   (:holder-identity lease)
   (some-> (:lease-transitions lease) str)
   token
   (some-> (remaining-ttl-ms lease now-ms) str)])

(defn- acquire-busy-result
  [{:keys [backend remaining-ttl-ms]} election-id lease now-ms]
  (common/acquire-result backend election-id
                         ["busy"
                          (:holder-identity lease)
                          (some-> (:lease-transitions lease) str)
                          ""
                          (some-> (remaining-ttl-ms lease now-ms) str)]))

(defn- renew-lost-result
  [{:keys [backend remaining-ttl-ms]} election-id lease now-ms]
  (if lease
    (common/renew-result backend election-id (holder-response :lost lease now-ms remaining-ttl-ms))
    (common/renew-result backend election-id ["lost"])))

(defn- resign-not-owner-result
  [{:keys [backend remaining-ttl-ms]} election-id lease now-ms]
  (if lease
    (common/resign-result backend election-id (holder-response :not-owner lease now-ms remaining-ttl-ms))
    (common/resign-result backend election-id ["not-owner"])))

(defn acquire!
  [{:keys [backend owner-id lease-name-prefix default-lease-ms clock conflict-marker
           lease-name get-lease create-lease! replace-lease!
           lease-active? create-lease-body acquire-lease-body remaining-ttl-ms] :as ctx}
   election-id
   opts]
  (let [election-id (common/normalize-election-id election-id)
        lease-name (lease-name lease-name-prefix election-id)
        lease-ms (common/lease-ms opts default-lease-ms)
        now-ms (common/now-ms clock)
        token (common/generate-token)
        current (get-lease lease-name)]
    (cond
      (nil? current)
      (let [created (create-lease! (create-lease-body lease-name owner-id token election-id now-ms lease-ms))]
        (if (= conflict-marker created)
          (if-let [lease (get-lease lease-name)]
            (acquire-busy-result ctx election-id lease now-ms)
            (common/acquire-result backend election-id ["busy"]))
          (common/acquire-result backend election-id
                                (acquired-response created token now-ms remaining-ttl-ms))))

      (lease-active? current now-ms)
      (acquire-busy-result ctx election-id current now-ms)

      :else
      (let [updated (replace-lease! lease-name (acquire-lease-body lease-name current owner-id token election-id now-ms lease-ms))]
        (if (= conflict-marker updated)
          (if-let [lease (get-lease lease-name)]
            (if (lease-active? lease now-ms)
              (acquire-busy-result ctx election-id lease now-ms)
              (common/acquire-result backend election-id ["busy"]))
            (common/acquire-result backend election-id ["busy"]))
          (common/acquire-result backend election-id
                                (acquired-response updated token now-ms remaining-ttl-ms)))))))

(defn renew!
  [{:keys [backend owner-id lease-name-prefix default-lease-ms clock conflict-marker
           lease-name get-lease replace-lease!
           lease-active? lease-owned-by-token? lease-owned-by-caller?
           renew-lease-body remaining-ttl-ms] :as ctx}
   election-id
   token
   opts]
  (let [election-id (common/normalize-election-id election-id)
        token (common/normalize-token token)
        lease-name (lease-name lease-name-prefix election-id)
        lease-ms (common/lease-ms opts default-lease-ms)
        now-ms (common/now-ms clock)
        current (get-lease lease-name)]
    (if (or (nil? current)
            (not (lease-owned-by-token? current token now-ms)))
      (renew-lost-result ctx election-id (when (lease-active? current now-ms) current) now-ms)
      (let [updated (replace-lease! lease-name (renew-lease-body lease-name current owner-id token election-id now-ms lease-ms))]
        (if (= conflict-marker updated)
          (let [lease (get-lease lease-name)]
            (if (and lease
                     (lease-owned-by-caller? lease owner-id token now-ms))
              (let [retry (replace-lease! lease-name (renew-lease-body lease-name lease owner-id token election-id now-ms lease-ms))]
                (if (= conflict-marker retry)
                  (renew-lost-result ctx election-id (when (lease-active? lease now-ms) lease) now-ms)
                  (common/renew-result backend election-id
                                       ["renewed"
                                        (:holder-identity retry)
                                        (some-> (:lease-transitions retry) str)
                                        token
                                        (some-> (remaining-ttl-ms retry now-ms) str)])))
              (renew-lost-result ctx election-id (when (lease-active? lease now-ms) lease) now-ms)))
          (common/renew-result backend election-id
                               ["renewed"
                                (:holder-identity updated)
                                (some-> (:lease-transitions updated) str)
                                token
                                (some-> (remaining-ttl-ms updated now-ms) str)]))))))

(defn resign!
  [{:keys [backend owner-id lease-name-prefix clock conflict-marker
           lease-name get-lease replace-lease!
           lease-active? lease-owned-by-token? lease-owned-by-caller?
           resign-lease-body remaining-ttl-ms] :as ctx}
   election-id
   token]
  (let [election-id (common/normalize-election-id election-id)
        token (common/normalize-token token)
        lease-name (lease-name lease-name-prefix election-id)
        now-ms (common/now-ms clock)
        current (get-lease lease-name)]
    (if (or (nil? current)
            (not (lease-owned-by-token? current token now-ms)))
      (resign-not-owner-result ctx election-id (when (lease-active? current now-ms) current) now-ms)
      (let [updated (replace-lease! lease-name (resign-lease-body lease-name current election-id))]
        (if (= conflict-marker updated)
          (let [lease (get-lease lease-name)]
            (if (and lease
                     (lease-owned-by-caller? lease owner-id token now-ms))
              (let [retry (replace-lease! lease-name (resign-lease-body lease-name lease election-id))]
                (if (= conflict-marker retry)
                  (resign-not-owner-result ctx election-id (when (lease-active? lease now-ms) lease) now-ms)
                  (common/resign-result backend election-id
                                        ["released"
                                         owner-id
                                         (some-> (:lease-transitions retry) str)])))
              (resign-not-owner-result ctx election-id (when (lease-active? lease now-ms) lease) now-ms)))
          (common/resign-result backend election-id
                                ["released"
                                 owner-id
                                 (some-> (:lease-transitions updated) str)]))))))

(defn status
  [{:keys [backend lease-name-prefix clock lease-name get-lease lease-active? remaining-ttl-ms]} election-id]
  (let [election-id (common/normalize-election-id election-id)
        lease-name (lease-name lease-name-prefix election-id)
        now-ms (common/now-ms clock)
        current (get-lease lease-name)]
    (if (and current (lease-active? current now-ms))
      (common/status-result backend election-id
                            (holder-response :held current now-ms remaining-ttl-ms))
      (common/status-result backend election-id ["vacant"]))))
