(ns d-core.libs.tryable)


;; tryable macro that returns a map with the result or error but silently to allow debugging
(defmacro tryable
  "A macro that wraps the body in a try-catch block and returns a map with the result or error."
  [& body]
  `(try
     {:result (do ~@body)}
     (catch Exception e#
       {:error e#})))

;; tryable macro that throws the error instead of returning it in a map but non silently to allow debugging
(defmacro tryable!
  "A macro that wraps the body in a try-catch block and returns the result or throws the error."
  [& body]
  `(try
     (do ~@body)
     (catch Exception e#
       (println "Error in tryable!: " (.getMessage e#))
       (throw e#))))

