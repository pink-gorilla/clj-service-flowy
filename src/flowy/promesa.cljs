(ns flowy.promesa
  (:require
   [taoensso.timbre :refer-macros [debug info error]]
   [promesa.core :as p]
   [missionary.core :as m]
   [reflower :refer [task]]))

(defn clj
  ([fun]
   (clj fun))
  ([fun & args]
   (let [t (apply task fun args)]
     (p/create
      (fn [resolve reject]
        (t resolve reject)
        )))))
