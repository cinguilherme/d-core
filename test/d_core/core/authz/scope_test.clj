(ns d-core.core.authz.scope-test
  (:require [clojure.test :refer [deftest is testing]]
            [d-core.core.authz.protocol :as p]
            [d-core.core.authz.scope :as scope]))

(deftest scope-authorizer-allows-with-matching-tenant-and-scopes
  (testing "Allows when tenant and scopes match"
    (let [authorizer (scope/->ScopeAuthorizer nil)
          principal {:tenant-id "tenant-1"
                     :scopes #{"messages:read" "messages:write"}}
          decision (p/authorize authorizer principal
                                {:tenant "tenant-1"
                                 :scopes ["messages:read"]}
                                nil)]
      (is (:allowed? decision)))))

(deftest scope-authorizer-denies-missing-scope
  (testing "Denies when required scopes are missing"
    (let [authorizer (scope/->ScopeAuthorizer nil)
          principal {:tenant-id "tenant-1"
                     :scopes #{"messages:read"}}
          decision (p/authorize authorizer principal
                                {:tenant "tenant-1"
                                 :scopes #{"messages:write"}}
                                nil)]
      (is (false? (:allowed? decision)))
      (is (= :missing-scope (:reason decision))))))

(deftest scope-authorizer-denies-tenant-mismatch
  (testing "Denies when tenant does not match"
    (let [authorizer (scope/->ScopeAuthorizer nil)
          principal {:tenant-id "tenant-1"
                     :scopes #{"messages:read"}}
          decision (p/authorize authorizer principal
                                {:tenant "tenant-2"
                                 :scopes #{"messages:read"}}
                                nil)]
      (is (false? (:allowed? decision)))
      (is (= :tenant-mismatch (:reason decision))))))

(deftest scope-authorizer-denies-unauthenticated
  (testing "Denies when principal is missing"
    (let [authorizer (scope/->ScopeAuthorizer nil)
          decision (p/authorize authorizer nil
                                {:tenant "tenant-1"
                                 :scopes #{"messages:read"}}
                                nil)]
      (is (false? (:allowed? decision)))
      (is (= :unauthenticated (:reason decision))))))
