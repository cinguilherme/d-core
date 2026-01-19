(ns d-core.core.metrics.protocol)

(defprotocol MetricsProtocol
  (registry [this] "Return the backing metrics registry.")
  (counter [this opts] "Create or fetch a counter. opts: {:name :help :labels}.")
  (gauge [this opts] "Create or fetch a gauge. opts: {:name :help :labels}.")
  (histogram [this opts] "Create or fetch a histogram. opts: {:name :help :labels :buckets}.")
  (inc! [this metric] [this metric amount] "Increment a counter or gauge.")
  (observe! [this histogram value] "Record a histogram observation."))
