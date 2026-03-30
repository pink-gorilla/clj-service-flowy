(ns flowy.reflower
  (:require
   [taoensso.timbre :refer [info]]
   [missionary.core :as m]
   [tick.core :as t]
   [clj-service.core :refer [call-fn get-service]]
   [clj-service.executor :refer [execute-wrapped]]
   [flowy.log :as l])
  (:import
   [missionary Cancelled]
   [java.time.format DateTimeFormatter]
   [java.time ZoneId]))

(def rfc-1123-formatter
  (-> (DateTimeFormatter/ofPattern "EEE, dd MMM yyyy HH:mm:ss 'GMT'")
      (.withZone (ZoneId/of "GMT"))))

(defn cookie-string [name value]
  (let [expiry-date (-> (t/zoned-date-time)
                        (t/>> (t/new-duration (* 365 10) :days))
                        (.format rfc-1123-formatter))]
    (str name "=" value "; expires=" expiry-date "; path=/;")))

(defn run-sp-clj [logger write ctx setup {:keys [id] :as clj-call}]
  (m/sp
   (l/log logger "start-sp:" clj-call)
   (let [v (m/? (execute-wrapped ctx setup clj-call))]
     (m/? (write (assoc v :op :exec :id id))))))

(defn start-ap [logger write service {:keys [id] :as clj-call}]
  (l/log logger "start-ap:" clj-call)
  (when-let [f (call-fn service clj-call)]
    (m/reduce (fn [_s v]
                (try
                  (m/? (write {:op :exec
                               :id id
                               :val v}))
                  (catch Cancelled _
                    (l/log logger "ap cancelled on ws close"))))
              nil f)))

(defn start-executing
  "returns a missionary task which can execute the clj-call"
  [logger write ctx setup {:keys [mode] :as service} {:keys [id] :as clj-call}]
  (case mode
    ; task-once for :sp/:clj
    :sp (run-sp-clj logger write ctx setup clj-call)
    :clj (run-sp-clj logger write ctx setup clj-call)
    ; flow consumer for :ap
    :ap (start-ap logger write service clj-call)
    (throw (ex-info (str "unknown mode: " mode) clj-call))))

(defn start-reflower [{:keys [identity browser-id ctx] :as req}]
    (info "reflower client wants to connect with ring-req: " (keys req))
    (let [setup {:user identity :session browser-id}
          logger (l/create-logger browser-id)
          clj (:clj ctx)]
      (l/log logger "\n\nsession started: " browser-id)
      (fn [write read]
        (let [msg-in (m/stream
                      (m/observe read))
              running (atom {})
              add-task (fn [id t]
                         (swap! running assoc id t))
              remove-task (fn [id]
                            (swap! running dissoc id))
              cancel-task (fn [task-id]
                            (if-let [dispose! (get @running task-id)]
                              (do  (l/log logger "cancelling task: " task-id)
                                   (dispose!)
                                   (remove-task task-id))
                              (l/log logger "task to cancel not found: " task-id)))
              cancel-all (fn []
                           (let [task-ids (keys @running)]
                             (l/log logger "cancelling running tasks: " task-ids)
                             (doall (map cancel-task task-ids))
                             (l/log logger "all running tasks cancelled!")))
              process-msg (fn [_state {:keys [op id] :as msg}]
                            (case op
                              nil
                              (l/log logger "ignoring msg without op: " msg)

                              :exec
                              (if-let [s (get-service clj msg)]
                                (if-let [t (start-executing logger write ctx setup s msg)]
                                  (add-task id (t
                                                (fn [r]
                                                  (l/log logger "task" msg "completed: " r)
                                                  (remove-task id))
                                                (fn [ex]
                                                  ;(l/log logger "task " msg "crashed: " ex)
                                                  (l/log logger "task " msg " crashed "
                                                         " ex-message: " (ex-message ex)
                                                         " ex-data: " (ex-data ex)
                                                         " ex-cause: " (ex-cause ex)
                                                         " ex full:" ex)
                                                  (remove-task id)
                                                  (m/? (write {:op :exec :id id :err (ex-message ex)})))))
                                  (do (l/log logger "start error: " msg)
                                      (m/? (write {:op :exec
                                                   :id id
                                                   :error "start error"}))))
                                (do (l/log logger "unkown service: " msg)
                                    (m/? (write {:op :exec
                                                 :id id
                                                 :error "unknown service"}))))
                              :cancel
                              (cancel-task id)
                            ; else
                              (l/log logger "unknown op: " msg)))]
          (m/sp
           (try
             (l/log logger "reflower task starting for websocket session")
             (m/? (write {:op :message
                          :val browser-id
                          :cookie (cookie-string "flowy-browser-id" browser-id)}))
             (m/?
              (m/reduce process-msg 0 msg-in))
             (l/log logger "reflower finished successfully!")

             (catch Exception ex
               (l/log logger "reflower crashed: " ex)
               (cancel-all))
             (catch Cancelled _
               (l/log logger "reflower got cancelled.")
               (cancel-all)
             ;(m/? shutdown!)
               true)))))))

(comment
  (cookie-string "a" "1")

 ; 
  )

