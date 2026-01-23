lint:
	clojure -M:lint --lint src

tests:
	clojure -M:test

integration-tests:
	INTEGRATION=1 clojure -M:test

build:
	clojure -T:build jar

publish:
	clojure -T:build deploy
