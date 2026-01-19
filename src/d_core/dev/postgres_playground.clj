(ns d-core.dev.postgres-playground
  (:require
   [clojure.java.jdbc :as jdbc]))

(def datasource {:jdbc-url "jdbc:postgresql://localhost:5432"
                 :username "postgres-playground"
                 :password "postgres-playground"})

(jdbc/execute! datasource ["CREATE DATABASE postgres-playground"
                           "CREATE USER postgres-playground WITH PASSWORD 'postgres-playground'"
                           "GRANT ALL PRIVILEGES ON DATABASE postgres-playground TO postgres-playground"])

(def db (jdbc/get-connection datasource))

(jdbc/execute! db ["CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(255))"])

(jdbc/execute! db ["INSERT INTO users (name) VALUES ('John')"])