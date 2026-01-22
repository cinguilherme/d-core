(ns build
  (:require [clojure.tools.build.api :as b]
            [deps-deploy.deps-deploy :as dd]))

(def lib 'org.clojars.cinguilherme/d-core)
(def version "0.1.0")
(def class-dir "target/classes")
(def basis (b/create-basis {:project "deps.edn"}))
(def jar-file (format "target/%s-%s.jar" (name lib) version))

(defn pom-path []
  (b/pom-path {:lib lib :class-dir class-dir}))

(defn clean [_]
  (b/delete {:path "target"}))

(defn jar [_]
  (clean nil)
  (b/copy-dir {:src-dirs ["src"] :target-dir class-dir})
  (b/write-pom {:class-dir class-dir
                :lib lib
                :version version
                :basis basis
                :src-dirs ["src"]
                :pom-data
                [[:licenses
                  [:license
                   [:name "MIT License"]
                   [:url "https://opensource.org/licenses/MIT"]]]
                 [:scm
                  [:url "https://github.com/cinguilherme/d-core"]
                  [:connection "scm:git:https://github.com/cinguilherme/d-core.git"]
                  [:developerConnection "scm:git:ssh://git@github.com/cinguilherme/d-core.git"]]
                 [:developers
                  [:developer
                   [:id "cinguilherme"]
                   [:name "Guilherme Cintra"]]]]} )
  (b/jar {:class-dir class-dir :jar-file jar-file}))

(defn deploy [_]
  (jar nil)
  (dd/deploy {:installer :remote
              :sign-releases? false
              :artifact jar-file
              :pom-file (pom-path)}))
