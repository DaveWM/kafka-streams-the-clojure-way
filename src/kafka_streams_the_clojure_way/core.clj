(ns kafka-streams-the-clojure-way.core
  (:require [jackdaw.streams :as js]
            [jackdaw.client :as jc]
            [jackdaw.client.log :as jcl]
            [jackdaw.admin :as ja]
            [jackdaw.serdes.edn :refer [serde]]
            [willa.streams :refer [transduce-stream]]
            [willa.core :as w]
            [willa.viz :as wv]
            [willa.experiment :as we]
            [willa.specs :as ws]
            [clojure.spec.alpha :as s]))

;; The config for our Kafka Streams app
(def kafka-config
  {"application.id" "kafka-streams-the-clojure-way"
   "bootstrap.servers" "localhost:9092"
   "default.key.serde" "jackdaw.serdes.EdnSerde"
   "default.value.serde" "jackdaw.serdes.EdnSerde"
   "cache.max.bytes.buffering" "0"})

;; Serdes tell Kafka how to serialize/deserialize messages
;; We'll just keep them as EDN
(def serdes
  {:key-serde (serde)
   :value-serde (serde)})

;; Each topic needs a config. The important part to note is the :topic-name key.
(def purchase-made-topic
  (merge {:topic-name "purchase-made"
          :partition-count 1
          :replication-factor 1
          :topic-config {}}
         serdes))

(def humble-donation-made-topic
  (merge {:topic-name "humble-donation-made"
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

;; An admin client is needed to do things like create and delete topics
(def admin-client (ja/->AdminClient kafka-config))


;; Part 1 - Simple Topology

(defn make-purchase! [amount]
  "Publish a message to the purchase-made topic, with the specified amount"
  (let [purchase-id (rand-int 10000)
        user-id     (rand-int 10000)
        quantity    (inc (rand-int 10))]
    (with-open [producer (jc/producer kafka-config serdes)]
      @(jc/produce! producer purchase-made-topic purchase-id {:id purchase-id
                                                              :amount amount
                                                              :user-id user-id
                                                              :quantity quantity}))))

(defn view-messages [topic]
  "View the messages on the given topic"
  (with-open [consumer (jc/subscribed-consumer (assoc kafka-config "group.id" (str (java.util.UUID/randomUUID)))
                                               [topic])]
    (jc/seek-to-beginning-eager consumer)
    (->> (jcl/log-until-inactivity consumer 100)
         (map :value)
         doall)))


(defn simple-topology [builder]
  (-> (js/kstream builder purchase-made-topic)
      (js/filter (fn [[_ purchase]]
                   (<= 100 (:amount purchase))))
      (js/map (fn [[key purchase]]
                [key (select-keys purchase [:amount :user-id])]))
      (js/to large-transaction-made-topic)))


(defn start! []
  "Starts the simple topology"
  (let [builder (js/streams-builder)]
    (simple-topology builder)
    (doto (js/kafka-streams builder kafka-config)
      (js/start))))

(defn stop! [kafka-streams-app]
  "Stops the given KafkaStreams application"
  (js/close kafka-streams-app))


;; Part 2 - Transducers

(def purchase-made-transducer
  (comp
    (filter (fn [[_ purchase]]
              (<= 100 (:amount purchase))))
    (map (fn [[key purchase]]
           [key (select-keys purchase [:amount :user-id])]))))

(defn simple-topology-with-transducer [builder]
  (-> (js/kstream builder purchase-made-topic)
      (transduce-stream purchase-made-transducer)
      (js/to large-transaction-made-topic)))

(def humble-donation-made-transducer
  (comp
    (filter (fn [[_ donation]]
              (<= 10000 (:donation-amount-cents donation))))
    (map (fn [[key donation]]
           [key {:user-id (:user-id donation)
                 :amount (int (/ (:donation-amount-cents donation) 100))}]))))

(defn make-humble-donation! [amount-cents]
  "Publishes a message to humble-donation-made, with the specified amount"
  (let [user-id (rand-int 10000)
        id      (rand-int 1000)]
    (with-open [producer (jc/producer kafka-config serdes)]
      @(jc/produce! producer humble-donation-made-topic id {:donation-amount-cents amount-cents
                                                            :user-id user-id
                                                            :donation-date "2019-01-01"}))))

(defn more-complicated-topology [builder]
  (js/merge
    (-> (js/kstream builder purchase-made-topic)
        (transduce-stream purchase-made-transducer))
    (-> (js/kstream builder humble-donation-made-topic)
        (transduce-stream humble-donation-made-transducer))))


;; Part 3 - Willa

(def entities
  {:topic/purchase-made (assoc purchase-made-topic ::w/entity-type :topic)
   :topic/humble-donation-made (assoc humble-donation-made-topic ::w/entity-type :topic)
   :topic/large-transaction-made (assoc large-transaction-made-topic ::w/entity-type :topic)

   :stream/large-purchase-made {::w/entity-type :kstream
                                ::w/xform purchase-made-transducer}
   :stream/large-donation-made {::w/entity-type :kstream
                                ::w/xform humble-donation-made-transducer}})

(def workflow
  [[:topic/purchase-made :stream/large-purchase-made]
   [:topic/humble-donation-made :stream/large-donation-made]
   [:stream/large-purchase-made :topic/large-transaction-made]
   [:stream/large-donation-made :topic/large-transaction-made]])

(def topology
  {:workflow workflow
   :entities entities})



(comment

  ;; Part 1 - Simple Topology


  ;; create the "purchase-made" and "large-transaction-made" topics
  (ja/create-topics! admin-client [purchase-made-topic large-transaction-made-topic])


  ;; Make a few dummy purchases
  (make-purchase! 10)
  (make-purchase! 500)
  (make-purchase! 50)
  (make-purchase! 1000)


  ;; View the purchases on the topic - there should be 4
  (view-messages purchase-made-topic)

  ;; Start the topology
  (def kafka-streams-app (start!))

  ;; You should see 2 messages on the large-transaction-made-topic topic
  (view-messages large-transaction-made-topic)

  ;; Stop the topology
  (stop! kafka-streams-app)


  ;; Part 2 - Transducers


  ;; Check that the purchase-made-transducer works as expected
  (into []
        purchase-made-transducer
        [[1 {:purchase-id 1 :user-id 2 :amount 10 :quantity 1}]
         [3 {:purchase-id 3 :user-id 4 :amount 500 :quantity 100}]])


  ;; Part 3 - Willa


  ;; Visualise the topology
  (wv/view-topology topology)

  ;; Start topology
  (let [builder (js/streams-builder)]
    (w/build-topology! builder topology)
    (js/start (js/kafka-streams builder kafka-config)))


  ;; Create the humble-donation-made topic
  (ja/create-topics! admin-client [humble-donation-made-topic])


  ;; Publish a couple of messages to the input topics
  (make-purchase! 200)
  (make-humble-donation! 15000)

  ;; Check that messages appear on the large-transaction-made output topics
  (view-messages large-transaction-made-topic)

  ;; Run an experiment
  (def experiment-results
    (we/run-experiment topology
                       {:topic/purchase-made [{:key 1
                                               :value {:id 1
                                                       :amount 200
                                                       :user-id 1234
                                                       :quantity 100}}]
                        :topic/humble-donation-made [{:key 2
                                                      :value {:user-id 2345
                                                              :donation-amount-cents 15000
                                                              :donation-date "2019-01-02"}}]}))

  ;; Visualise experiment result
  (wv/view-topology experiment-results)

  ;; View results as data
  (->> experiment-results
       :entities
       (map (fn [[k v]]
              [k (::we/output v)]))
       (into {}))

  ;; Validate topology
  (s/explain ::ws/topology topology)

  ;; Check that the spec validation will catch an invalid topology
  (s/explain ::ws/topology
             ;; introduce a loop in our workflow
             (update topology :workflow conj [:topic/large-transaction-made :topic/purchase-made]))
  )
