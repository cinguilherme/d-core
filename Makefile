BUMP ?= patch

lint:
	clojure -M:lint --lint src

tests:
	clojure -M:test

integration-tests:
	INTEGRATION=1 clojure -M:test

build:
	clojure -T:build jar

publish:
	clojure -T:build publish :bump :$(BUMP)

publish-major:
	$(MAKE) publish BUMP=major

publish-minor:
	$(MAKE) publish BUMP=minor

publish-patch:
	$(MAKE) publish BUMP=patch
