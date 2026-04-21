(ns d-core.core.leader-election.logics.postgres
  (:require [d-core.core.leader-election.common :as common]))

(defn- holder-row->response
  [status row]
  (when row
    [(name status)
     (:owner_id row)
     (str (:fencing row))
     (str (:remaining_ttl_ms row))]))

(defn acquire!
  [{:keys [backend datasource table-ident owner-id default-lease-ms try-acquire-row! select-active-holder]} election-id opts]
  (let [election-id (common/normalize-election-id election-id)
        token (common/generate-token)
        lease-ms (common/lease-ms opts default-lease-ms)]
    (if-let [row (try-acquire-row! datasource table-ident election-id owner-id token lease-ms)]
      (common/acquire-result backend election-id
                             ["acquired"
                              (:owner_id row)
                              (str (:fencing row))
                              (:token row)
                              (str (:remaining_ttl_ms row))])
      (if-let [holder (select-active-holder datasource table-ident election-id)]
        (common/acquire-result backend election-id
                               ["busy"
                                (:owner_id holder)
                                (str (:fencing holder))
                                ""
                                (str (:remaining_ttl_ms holder))])
        (common/acquire-result backend election-id ["busy"])))))

(defn renew!
  [{:keys [backend datasource table-ident default-lease-ms try-renew-row! select-active-holder]} election-id token opts]
  (let [election-id (common/normalize-election-id election-id)
        token (common/normalize-token token)
        lease-ms (common/lease-ms opts default-lease-ms)]
    (if-let [row (try-renew-row! datasource table-ident election-id token lease-ms)]
      (common/renew-result backend election-id
                           ["renewed"
                            (:owner_id row)
                            (str (:fencing row))
                            (:token row)
                            (str (:remaining_ttl_ms row))])
      (if-let [holder (select-active-holder datasource table-ident election-id)]
        (common/renew-result backend election-id
                             (holder-row->response :lost holder))
        (common/renew-result backend election-id ["lost"])))))

(defn resign!
  [{:keys [backend datasource table-ident owner-id try-resign-row! select-active-holder]} election-id token]
  (let [election-id (common/normalize-election-id election-id)
        token (common/normalize-token token)]
    (if-let [row (try-resign-row! datasource table-ident election-id token)]
      (common/resign-result backend election-id
                            ["released"
                             owner-id
                             (str (:fencing row))])
      (if-let [holder (select-active-holder datasource table-ident election-id)]
        (common/resign-result backend election-id
                              (holder-row->response :not-owner holder))
        (common/resign-result backend election-id ["not-owner"])))))

(defn status
  [{:keys [backend datasource table-ident select-active-holder]} election-id]
  (let [election-id (common/normalize-election-id election-id)]
    (if-let [holder (select-active-holder datasource table-ident election-id)]
      (common/status-result backend election-id
                            (holder-row->response :held holder))
      (common/status-result backend election-id ["vacant"]))))
