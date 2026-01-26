(ns d-core.libs.workers
  (:require [clojure.core.async :as async]))

(defn channel-buffer
  "Return a core.async buffer for channel config.

  Supported keys:
  - :buffer (size, default 0)
  - :buffer-type (:fixed | :dropping | :sliding, default :fixed)
  A :buffer of 0 returns an unbuffered channel."
  [{:keys [buffer buffer-type]}]
  (let [size (long (or buffer 0))
        buffer-type (or buffer-type :fixed)]
    (cond
      (zero? size) nil
      (= buffer-type :dropping) (async/dropping-buffer size)
      (= buffer-type :sliding) (async/sliding-buffer size)
      (= buffer-type :fixed) (async/buffer size)
      :else (throw (ex-info "Unknown buffer-type"
                            {:buffer-type buffer-type
                             :buffer buffer})))))

(defn make-channels
  "Build a map of channel-id -> core.async channel from a channels config map."
  [channels]
  (reduce-kv
    (fn [m k cfg]
      (let [buf (channel-buffer cfg)]
        (assoc m k (if buf (async/chan buf) (async/chan)))))
    {}
    channels))

(defn worker-ctx
  "Build the worker context map passed to worker functions."
  [components channels worker-id worker]
  {:components components
   :channels channels
   :worker (assoc worker :id worker-id)})

(defn bump!
  "Increment a stats counter at the given nested path."
  [stats path]
  (when stats
    (swap! stats update-in path (fnil inc 0))))

(defn emit!
  "Emit an event through the provided emitter, if any."
  [emit event]
  (when emit
    (emit event)))

(defn record-drop!
  "Record a drop counter and emit a :workers.drop event."
  [stats emit {:keys [worker-id channel-id reason]}]
  (bump! stats [:drops channel-id])
  (emit! emit {:event/type :workers.drop
               :worker-id worker-id
               :channel channel-id
               :reason reason}))

(defn error-event
  "Build a structured error event from worker context + exception."
  [ctx msg ex]
  {:worker-id (get-in ctx [:worker :id])
   :worker-kind (get-in ctx [:worker :kind])
   :message (or (.getMessage ex) "No message")
   :ex-data (ex-data ex)
   :error (Throwable->map ex)
   :msg msg})

(defn record-error!
  "Record an error counter and emit a :workers.error event."
  [stats emit event]
  (bump! stats [:errors (:worker-id event)])
  (emit! emit (assoc (dissoc event :error)
                     :event/type :workers.error)))

(defn resolve-emitter
  "Resolve an emitter function from components.

  Accepted shapes:
  - components {:observability <fn|{:emit fn}>}
  - components {:obs <fn|{:emit fn}>}
  - components {:logger <fn|{:log fn}>}"
  [components]
  (let [c (or (:observability components)
              (:obs components)
              (:logger components))]
    (cond
      (fn? c) c
      (and (map? c) (fn? (:emit c))) (:emit c)
      (and (map? c) (fn? (:log c))) (:log c)
      :else nil)))

(defn guarded-go-call
  "Run a worker function inside a go block and fail if it exceeds guard-ms.

  This must be invoked from within a go context."
  [worker-fn ctx msg {:keys [guard-ms]}]
  (let [guard-ms (or guard-ms 25)
        done (async/go (worker-fn ctx msg))
        timeout (async/timeout guard-ms)
        [result-val result-port] (async/alts! [done timeout])
        result (if (= result-port done)
                 [:done result-val]
                 [:timeout nil])]
    (when (= (first result) :timeout)
      (throw (ex-info "Blocking call detected in :dispatch :go worker"
                      {:worker-id (get-in ctx [:worker :id])
                       :guard-ms guard-ms})))
    (second result)))

(defn deliver-output!
  "Attempt to deliver a worker result to an output channel without blocking.

  When the output channel is full, records a drop and emits an event."
  [out-chan value {:keys [worker-id output-chan-id stats emit]}]
  (when (and out-chan (some? value))
    (when-not (async/offer! out-chan value)
      (record-drop! stats emit {:worker-id worker-id
                                :channel-id output-chan-id
                                :reason :output-drop}))))

(defn handle-worker-error!
  "Handle a worker error by emitting a structured event and optionally
  forwarding it to a fail channel."
  [ctx msg ex {:keys [stats emit fail-chan fail-chan-id]}]
  (let [event (error-event ctx msg ex)]
    (record-error! stats emit event)
    (when fail-chan
      (when-not (async/offer! fail-chan (assoc event :fail-chan fail-chan-id))
        (record-drop! stats emit {:worker-id (:worker-id event)
                                  :channel-id fail-chan-id
                                  :reason :fail-chan-full})))))

(defn start-ticker-worker
  "Start a ticker worker that emits ticks to :out at :interval-ms until stop.

  Tick delivery is best-effort and non-blocking; when the output channel is
  full, ticks may be dropped and a drop event recorded (if stats/emit are
  provided)."
  [worker ctx stop-chan]
  (let [{:keys [out interval-ms]} worker
        {:keys [stats emit]} ctx
        out-chan (get (:channels ctx) out)
        interval-ms (or interval-ms 1000)
        worker-id (get-in ctx [:worker :id])
        output-chan-id out]
    (async/go-loop [tick 0]
      (let [timeout (async/timeout interval-ms)
            [_ ch] (async/alts! [stop-chan timeout])]
        (when-not (= ch stop-chan)
          (when out-chan
            (when-not (async/offer! out-chan tick)
              (record-drop! stats emit {:worker-id worker-id
                                        :channel-id output-chan-id
                                        :reason :ticker-output-drop})))
          (recur (inc tick)))))))

(defn start-command-worker
  "Start a command worker that consumes from :in and invokes :worker-fn.

  Supported keys on worker:
  - :dispatch (:go | :thread, default :go)
  - :output-chan (optional channel-id for results)
  - :fail-chan (optional channel-id for error events)"
  [worker ctx {:keys [dev-guard? guard-ms stats emit]}]
  (let [{:keys [in worker-fn dispatch output-chan fail-chan]} worker
        in-chan (get (:channels ctx) in)
        out-chan (when output-chan (get (:channels ctx) output-chan))
        fail-chan* (when fail-chan (get (:channels ctx) fail-chan))
        dispatch (or dispatch :go)]
    (when-not worker-fn
      (throw (ex-info "Command worker requires :worker-fn"
                      {:worker-id (get-in ctx [:worker :id])})))
    (when (and output-chan (nil? out-chan))
      (throw (ex-info "Output channel not found"
                      {:worker-id (get-in ctx [:worker :id])
                       :output-chan output-chan})))
    (when (and fail-chan (nil? fail-chan*))
      (throw (ex-info "Fail channel not found"
                      {:worker-id (get-in ctx [:worker :id])
                       :fail-chan fail-chan})))
    (case dispatch
      :thread (async/thread
                (loop []
                  (when-some [msg (async/<!! in-chan)]
                    (try
                      (let [result (worker-fn ctx msg)]
                        (deliver-output! out-chan result {:worker-id (get-in ctx [:worker :id])
                                                          :output-chan-id output-chan
                                                          :stats stats
                                                          :emit emit}))
                      (catch Throwable t
                        (handle-worker-error! ctx msg t {:stats stats
                                                         :emit emit
                                                         :fail-chan fail-chan*
                                                         :fail-chan-id fail-chan})))
                    (recur))))
      :go (async/go-loop []
            (when-some [msg (async/<! in-chan)]
              (try
                (let [result (if dev-guard?
                               (guarded-go-call worker-fn ctx msg {:guard-ms guard-ms})
                               (worker-fn ctx msg))]
                  (deliver-output! out-chan result {:worker-id (get-in ctx [:worker :id])
                                                    :output-chan-id output-chan
                                                    :stats stats
                                                    :emit emit}))
                (catch Throwable t
                  (handle-worker-error! ctx msg t {:stats stats
                                                   :emit emit
                                                   :fail-chan fail-chan*
                                                   :fail-chan-id fail-chan})))
              (recur)))
      (throw (ex-info "Unknown dispatch" {:dispatch dispatch})))))

(defn exposed-channels
  "Return a map of exposed worker-id -> {:channel ... :channel-id ... :put-mode ...}."
  [definition channels]
  (reduce-kv
    (fn [m worker-id {:keys [kind in expose?]}]
      (if (and (= kind :command) expose? in)
        (let [cfg (get-in definition [:channels in])]
          (assoc m worker-id {:channel (get channels in)
                              :channel-id in
                              :put-mode (:put-mode cfg)}))
        m))
    {}
    (:workers definition)))

(defn start-workers
  "Start a workers runtime.

  Returns {:channels ... :exposed ... :stats ... :emit ... :stop! ...}."
  ([definition components]
   (start-workers definition components {}))
  ([definition components {:keys [dev-guard? guard-ms]}]
   (let [{:keys [channels workers]} definition
         channels (make-channels channels)
         stop-chan (async/chan)
         stats (atom {:drops {} :errors {}})
         emit (resolve-emitter components)]
     (doseq [[worker-id worker] workers]
       (let [worker (assoc worker :id worker-id)
             ctx (assoc (worker-ctx components channels worker-id worker)
                        :stats stats
                        :emit emit)]
         (case (:kind worker)
           :ticker (start-ticker-worker worker ctx stop-chan)
           :command (start-command-worker worker ctx {:dev-guard? dev-guard?
                                                      :guard-ms guard-ms
                                                      :stats stats
                                                      :emit emit})
           (throw (ex-info "Unknown worker kind"
                           {:kind (:kind worker)
                            :worker-id worker-id})))))
     {:channels channels
      :exposed (exposed-channels definition channels)
      :stats stats
      :emit emit
      :stop! (fn []
               (async/close! stop-chan)
               (doseq [[_ ch] channels]
                 (async/close! ch)))})))

(defn command!
  "Send a message to an exposed worker using its configured :put-mode.

  :put-mode can be:
  - :async (default) -> async/put!
  - :block -> blocking >!!
  - :drop -> non-blocking offer! with drop event"
  [system worker-id msg]
  (when-let [entry (get-in system [:exposed worker-id])]
    (if (map? entry)
      (let [{:keys [channel put-mode channel-id]} entry
            put-mode (or put-mode :async)]
        (case put-mode
          :block (async/>!! channel msg)
          :drop (when-not (async/offer! channel msg)
                  (record-drop! (:stats system) (:emit system)
                                {:worker-id worker-id
                                 :channel-id channel-id
                                 :reason :input-drop}))
          :async (async/put! channel msg)
          (async/put! channel msg)))
      (async/put! entry msg))))

(defn exposed-chan
  "Return the underlying channel for an exposed worker."
  [system worker-id]
  (let [entry (get-in system [:exposed worker-id])]
    (if (map? entry) (:channel entry) entry)))

(defn request!
  "Send a message with a reply-chan and return the reply-chan."
  [system worker-id msg]
  (let [reply-chan (async/promise-chan)]
    (command! system worker-id (assoc msg :reply-chan reply-chan))
    reply-chan))

(defn stats-snapshot
  "Return a snapshot of stats counters for drops and errors."
  [system]
  (when-let [stats (:stats system)]
    @stats))
