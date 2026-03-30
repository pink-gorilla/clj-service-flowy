(ns flowy.reflower
  (:require
   [missionary.core :as m]
   [taoensso.timbre :refer [debug debugf info infof warn error]]
   [flowy.client :refer [boot-with-retry connector]])
  (:import missionary.Cancelled))

(def out-mbx (m/mbx))
(def in-mbx (m/mbx))

(defn set-cookie [{:keys [cookie message]}]
  (info "message: " message)
  (when cookie
    (info "setting cookie: " cookie)
    (set! (.-cookie js/document) cookie)))

(defn conn-interactor [write read]
  ; this fn gets called whenever the connection is established.
  (info "conn-interactor ws established")
  (let [msg-in (m/stream
                (m/observe read))
        send-msg (m/sp
                  (loop [v (m/? out-mbx)]
                    (info "interactor send: " v)
                    (m/? (write v))
                    (recur (m/? out-mbx))))
        process-msg (m/reduce
                     (fn [_ msg]
                       (info "interactor rcvd: " msg)
                       ; {:val true-queen-8, :cookie flowy-browser-id=true-queen-8; expires=Sat, 31 Mar 2035 01:08:34 GMT; path=/;, :op :message}
                       (when (= :message (:op msg))
                         (set-cookie msg))
                       (in-mbx msg))
                     nil msg-in)]
    (m/sp
     (try
       (m/? (write {:op :message :val "browser-ws-connected"}))
       (m/? (m/join vector process-msg send-msg))
       (info "wsconninteractor DONE! success!")
       (catch js/Exception ex
         (info "wsconninteractor crashed: " ex))
       (catch Cancelled _
         (info "wsconninteractor was cancelled.")
             ;(m/? shutdown!)
         true)))))

(defn create-multiplexer []
  (let [req-id (atom 0)
        dispose! ((boot-with-retry conn-interactor connector)
                  #(info "multiplexer finished success:" %)
                  #(info "multiplexer finished error:" %))
        msg-flow (m/stream
                  (m/ap
                   (loop [msg (m/? in-mbx)]
                      ;(println "multiplexer rcvd: " msg)
                     (m/amb msg (recur (m/? in-mbx))))))]
    {:req-id req-id
     :dispose-fn dispose!
     :msg-flow msg-flow}))

(defn get-req-id [{:keys [req-id]}]
  (swap! req-id inc)
  @req-id)

(defn make-req
  "returns a task"
  [mx msg]
  (let [id (get-req-id mx)
        msg* (merge msg
                    {:op :exec
                     :id id})]
    (info "making request: " msg*)
    (out-mbx msg*)))

(def mx (create-multiplexer))

(defn task [fun & args]
  (m/sp
   (let [id (get-req-id mx)
         msg (if (seq args)
               {:op :exec
                :id id
                :fun fun
                :args args}
               {:op :exec
                :id id
                :fun fun})
          ;  {:val Don’t pursue happiness – create it., :op :exec, :id 1}
         match-id (fn [msg]
                     ;(println "matching: " msg)
                    (= (:id msg) id))
         first-result-msg-f (m/eduction
                             (filter match-id)
                             (take 1)
                             (:msg-flow mx))]
      ; first send the message
     (info "making req: " msg)
     (out-mbx msg)
     (info "req sent!")
      ; wait until msg received
     (let [msgs (m/? (m/reduce conj [] first-result-msg-f))
           {:keys [result error] :as full} (first msgs)]
       (info "full result:" full)
       (if result
         result
         (throw (ex-info (:message error) (or (:data error) {}))))
        ;(println "task msg: " full)
       )
      ;(m/? (m/sleep 10000))
      ;(println "task sleep done.")
     )))

(defn flow [fun & args]
  (m/ap
   (let [id (get-req-id mx)
         msg (if (seq args)
               {:op :exec
                :id id
                :fun fun
                :args args}
               {:op :exec
                :id id
                :fun fun})
         match-id (fn [msg]
                    (= (:id msg) id))
         result-msg-f (m/eduction
                       (filter match-id)
                       (:msg-flow mx))
         ;flow-forwarder (m/reduce (fn [_ v]
         ;                     (println "flow value: " v)
         ;                     (m/amb (:val msg)))
         ;                   nil
         ;                   result-msg-f)
         ]
      ; first send the message
     (info "making req: " msg)
     (out-mbx msg)
     (info "req sent!")
      ; wait until msg received
     ;(loop [msg (m/? result-msg-f)]
     ;  (m/amb (:val msg) (recur (m/? in-mbx))))
     ;(m/? flow-forwarder)
     (try (let [msg (m/?> result-msg-f)]
            (m/amb (:val msg)))
          (catch Cancelled c
            (info "unsubscribing flow!")
            (out-mbx {:op :cancel
                      :id id})
            (throw c))))))