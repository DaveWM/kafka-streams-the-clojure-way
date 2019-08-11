(ns kafka-streams-the-clojure-way.core
  (:require [jackdaw.streams :as js]
            [jackdaw.client :as jc]
            [jackdaw.client.log :as jcl]
            [jackdaw.admin :as ja]
            [jackdaw.serdes.edn :refer [serde]]))


(def kafka-config
  {"application.id" "kafka-streams-the-clojure-way"
   "bootstrap.servers" "localhost:9092"
   "default.key.serde" "jackdaw.serdes.EdnSerde"
   "default.value.serde" "jackdaw.serdes.EdnSerde"
   "cache.max.bytes.buffering" "0"})

(def serdes
  {:key-serde (serde)
   :value-serde (serde)})

(def purchase-made-topic
  (merge {:topic-name "purchase-made"
          :partition-count 1
          :replication-factor 1
          :topic-config {}}
         serdes))

(def large-transaction-made-topic
  (merge {:topic-name "large-transaction-made"
          :partition-count 1
          :replication-factor 1
          :topic-config {}}
         serdes))


(def admin-client (ja/->AdminClient kafka-config))


(defn make-purchase! [amount]
  (let [purchase-id (rand-int 10000)
        user-id     (rand-int 10000)
        quantity    (inc (rand-int 10))]
    (with-open [producer (jc/producer kafka-config serdes)]
      @(jc/produce! producer purchase-made-topic purchase-id {:id purchase-id
                                                              :amount amount
                                                              :user-id user-id
                                                              :quantity quantity}))))

;; TODO - maybe get rid of this - use the UI instead
(defn view-messages [topic]
  (with-open [consumer (jc/subscribed-consumer (assoc kafka-config "group.id" (str (java.util.UUID/randomUUID)))
                                               [topic])]
    (jc/seek-to-beginning-eager consumer)
    (->> (jcl/log-until-inactivity consumer 100)
         (map :value)
         doall)))


(defn build-topology [builder]
  (-> (js/kstream builder purchase-made-topic)
      (js/filter (fn [[_ purchase]]
                   (<= 100 (:amount #spy/p purchase))))
      (js/map-values (fn [purchase]
                       (select-keys purchase [:amount :user-id])))
      (js/to large-transaction-made-topic)))


(defn start! []
  (let [builder (js/streams-builder)]
    (build-topology builder)
    (doto (js/kafka-streams builder kafka-config)
      (js/start))))

(defn stop! [kafka-streams-app]
  (js/close kafka-streams-app))



(comment

  ;; create the "purchase-made" and "large-transaction-made" topics
  (ja/create-topics! admin-client [purchase-made-topic large-transaction-made-topic])


  ;; Make a few dummy purchases
  (make-purchase! 10)
  (make-purchase! 500)
  (make-purchase! 50)
  (make-purchase! 1000)


  ;; View the purchases on the topic - there should be 4
  (view-messages purchase-made-topic)
  )