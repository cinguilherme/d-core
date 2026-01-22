lint:
	clj-kondo --lint src

test:
	clojure -M:test

build:
	clojure -T:build jar

publish:
	clojure -T:build deploy