(ns kafka-streams-the-clojure-way.core
  (:require [jackdaw.streams :as js]
            [jackdaw.client :as jc]
            [jackdaw.serdes.edn :refer [serde]]))


(def kafka-config
  {"application.id"            "kafka-streams-the-clojure-way"
   "bootstrap.servers"         "localhost:9092"
   "default.key.serde"         "jackdaw.serdes.EdnSerde"
   "default.value.serde"       "jackdaw.serdes.EdnSerde"
   "cache.max.bytes.buffering" "0"})


(defn make-purchase! [amount]
  (let [producer (jc/producer kafka-config
                              {:key-serde   (serde)
                               :value-serde (serde)})
        purchase-id (rand-int 10000)]
    (jc/produce! producer {:topic-name "topic"} purchase-id {:id purchase-id
                                                             :amount amount})))



(comment

  ;; Publish a few
  (make-purchase! 10)
  (make-purchase! 500)
  (make-purchase! 50)
  (make-purchase! 1000)

  )