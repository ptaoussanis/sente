(ns taoensso.sente
  "Channel sockets for Clojure/Script.

      Protocol  | client>server | client>server ?+ ack/reply | server>user push
    * WebSockets:       ✓              [1]                           ✓
    * Ajax:            [2]              ✓                           [3]

    [1] Emulate with cb-uuid wrapping
    [2] Emulate with dummy-cb wrapping
    [3] Emulate with long-polling

  Abbreviations:
    * chsk      - Channel socket (Sente's own pseudo \"socket\")
    * server-ch - Underlying web server's async channel that implement
                  Sente's server channel interface
    * sch       - server-ch alias
    * uid       - User-id. An application-level user identifier used for async
                  push. May have semantic meaning (e.g. username, email address),
                  may not (e.g. client/random id) - app's discretion.
    * cb        - Callback
    * tout      - Timeout
    * ws        - WebSocket/s
    * pstr      - Packed string. Arbitrary Clojure data serialized as a
                  string (e.g. edn) for client<->server comms

  Special messages:
    * Callback wrapping: [<clj> <?cb-uuid>] for [1], [2]
    * Callback replies: :chsk/closed, :chsk/timeout, :chsk/error
    * Client-side events:
        [:chsk/handshake [<?uid> <?csrf-token> <?handshake-data> <first?>]]
        [:chsk/state <new-state-map>]
        [:chsk/recv <ev-as-pushed-from-server>] ; server>user push
        [:chsk/ws-error <websocket-error>] ; Experimental, subject to change

    * Server-side events:
        [:chsk/ws-ping]
        [:chsk/bad-package <packed-str>]
        [:chsk/bad-event   <chsk-event>]
        [:chsk/uidport-open]
        [:chsk/uidport-close]

  Notable implementation details:
    * core.async is used liberally where brute-force core.async allows for
      significant implementation simplifications. We lean on core.async's strong
      efficiency here.
    * For WebSocket fallback we use long-polling rather than HTTP 1.1 streaming
      (chunked transfer encoding). Http-kit _does_ support chunked transfer
      encoding but a small minority of browsers &/or proxies do not. Instead of
      implementing all 3 modes (WebSockets, streaming, long-polling) - it seemed
      reasonable to focus on the two extremes (performance + compatibility). In
      any case client support for WebSockets is growing rapidly so fallback
      modes will become increasingly irrelevant while the extra simplicity will
      continue to pay dividends.

  General-use notes:
    * Single HTTP req+session persists over entire chsk session but cannot
      modify sessions! Use standard a/sync HTTP Ring req/resp for logins, etc.
    * Easy to wrap standard HTTP Ring resps for transport over chsks. Prefer
      this approach to modifying handlers (better portability)."

  {:author "Peter Taoussanis (@ptaoussanis)"}

  #?(:clj
     (:require
       [clojure.string :as str]
       [clojure.core.async :as async :refer (<! <!! >! >!! put! chan go go-loop)]
       [taoensso.encore :as enc :refer (swap-in! reset-in! swapped have have! have?)]
       [taoensso.timbre :as timbre :refer (tracef debugf infof warnf errorf)]
       [taoensso.sente.interfaces :as interfaces]))

  #?(:cljs
     (:require
       [clojure.string :as str]
       [cljs.core.async :as async :refer (<! >! put! chan)]
       [taoensso.encore :as enc :refer (format swap-in! reset-in! swapped)
        :refer-macros (have have! have?)]
       [taoensso.timbre :as timbre :refer-macros (tracef debugf infof warnf errorf)]
       [taoensso.sente.interfaces :as interfaces]))

  #?(:cljs
     (:require-macros
       [cljs.core.async.macros :as asyncm :refer (go go-loop)])))

(if (vector? taoensso.encore/encore-version)
  (enc/assert-min-encore-version [2 52 1])
  (enc/assert-min-encore-version  2.52))

;; (timbre/set-level! :trace) ; Uncomment for debugging

;;;; Events
;; Clients & server both send `event`s and receive (i.e. route) `event-msg`s:
;;   - `event`s have the same form client+server side,
;;   - `event-msg`s have a similar but not identical form

(defn- validate-event [x]
  (cond
    (not (vector? x))        :wrong-type
    (not (#{1 2} (count x))) :wrong-length
    :else (let [[ev-id _] x]
            (cond (not (keyword? ev-id))  :wrong-id-type
                  (not (namespace ev-id)) :unnamespaced-id
                  :else nil))))

(defn event? "Valid [ev-id ?ev-data] form?" [x] (nil? (validate-event x)))

(defn as-event [x] (if (event? x) x [:chsk/bad-event x]))

(defn assert-event [x]
  (when-let [?err (validate-event x)]
    (let [err-msg
          (str
            (case ?err
              :wrong-type   "Malformed event (wrong type)."
              :wrong-length "Malformed event (wrong length)."
              (:wrong-id-type :unnamespaced-id)
              "Malformed event (`ev-id` should be a namespaced keyword)."
              :else "Malformed event (unknown error).")
            " Event should be of `[ev-id ?ev-data]` form: " x)]
      (throw (ex-info err-msg {:malformed-event x})))))

(defn client-event-msg? [x]
  (and
    (map? x)
    (enc/keys= x #{:ch-recv :send-fn :state :event :id :?data})
    (let [{:keys [ch-recv send-fn state event]} x]
      (and
        (enc/chan? ch-recv)
        (ifn?      send-fn)
        (enc/atom? state)
        (event?    event)))))

(defn server-event-msg? [x]
  (and
    (map? x)
    (enc/keys= x #{:ch-recv :send-fn :connected-uids
                   :ring-req :client-id
                   :event :id :?data :?reply-fn :uid})
    (let [{:keys [ch-recv send-fn connected-uids
                  ring-req client-id event ?reply-fn]} x]
      (and
        (enc/chan?       ch-recv)
        (ifn?            send-fn)
        (enc/atom?       connected-uids)
        (map?            ring-req)
        (enc/nblank-str? client-id)
        (event?          event)
        (or (nil? ?reply-fn)
            (ifn? ?reply-fn))))))

(defn- put-server-event-msg>ch-recv!
  "All server `event-msg`s go through this"
  [ch-recv {:as ev-msg :keys [event ?reply-fn]}]
  (let [[ev-id ev-?data :as valid-event] (as-event event)
        ev-msg* (merge ev-msg {:event     valid-event
                               :?reply-fn ?reply-fn
                               :id        ev-id
                               :?data     ev-?data})]
    (if-not (server-event-msg? ev-msg*)
      (warnf "Bad ev-msg: %s" ev-msg) ; Log 'n drop
      (put! ch-recv ev-msg*))))

;;; Note that cb replys need _not_ be `event` form!
#?(:cljs (defn cb-error?   [cb-reply-clj] (#{:chsk/closed :chsk/timeout :chsk/error} cb-reply-clj)))
#?(:cljs (defn cb-success? [cb-reply-clj] (not (cb-error? cb-reply-clj))))

;;;; Packing
;; * Client<->server payloads are arbitrary Clojure vals (cb replies or events).
;; * Payloads are packed for client<->server transit.
;; * Packing includes ->str encoding, and may incl. wrapping to carry cb info.

(defn- unpack "prefixed-pstr->[clj ?cb-uuid]"
  [packer prefixed-pstr]
  (have? string? prefixed-pstr)
  (let [wrapped? (enc/str-starts-with? prefixed-pstr "+")
        pstr     (subs prefixed-pstr 1)
        clj
        (try
          (interfaces/unpack packer pstr)
          (catch #?(:clj Throwable :cljs :default) t
                 (debugf "Bad package: %s (%s)" pstr t)
            [:chsk/bad-package pstr]))

        [clj ?cb-uuid] (if wrapped? clj [clj nil])
        ?cb-uuid (if (= 0 ?cb-uuid) :ajax-cb ?cb-uuid)]

    (tracef "Unpacking: %s -> %s" prefixed-pstr [clj ?cb-uuid])
    [clj ?cb-uuid]))

(defn- with-?meta [x ?m] (if (seq ?m) (with-meta x ?m) x))

(defn- pack "clj->prefixed-pstr"
  ([packer ?packer-meta clj]
   (let [pstr
         (str "-" ; => Unwrapped (no cb metadata)
           (interfaces/pack packer (with-?meta clj ?packer-meta)))]
     (tracef "Packing (unwrapped): %s -> %s" [?packer-meta clj] pstr)
     pstr))

  ([packer ?packer-meta clj ?cb-uuid]
   (let [;;; Keep wrapping as light as possible:
         ?cb-uuid    (if (= ?cb-uuid :ajax-cb) 0 ?cb-uuid)
         wrapped-clj (if ?cb-uuid [clj ?cb-uuid] [clj])
         pstr
         (str "+" ; => Wrapped (cb metadata)
           (interfaces/pack packer (with-?meta wrapped-clj ?packer-meta)))]
     (tracef "Packing (wrapped): %s -> %s" [?packer-meta clj ?cb-uuid] pstr)
     pstr)))

(deftype EdnPacker []
  interfaces/IPacker
  (pack   [_ x] (enc/pr-edn   x))
  (unpack [_ s] (enc/read-edn s)))

(def ^:private default-edn-packer (EdnPacker.))

(defn- coerce-packer [x]
  (if (enc/kw-identical? x :edn)
    default-edn-packer
    (have #(satisfies? interfaces/IPacker %) x)))

(comment
  (do
    (require '[taoensso.sente.packers.transit :as transit])
    (def ^:private default-transit-json-packer (transit/get-transit-packer)))

  (let [pack   interfaces/pack
        unpack interfaces/unpack
        data   {:a :A :b :B :c "hello world"}]

    (enc/qb 10000
      (let [pk default-edn-packer]          (unpack pk (pack pk data)))
      (let [pk default-transit-json-packer] (unpack pk (pack pk data))))))

;;;; Server API

(declare
  ^:private send-buffered-server-evs>ws-clients!
  ^:private send-buffered-server-evs>ajax-clients!)

(defn make-channel-socket-server!
  "Takes a web server adapter[1] and returns a map with keys:
    :ch-recv ; core.async channel to receive `event-msg`s (internal or from clients).
    :send-fn ; (fn [user-id ev] for server>user push.
    :ajax-post-fn                ; (fn [ring-req]) for Ring CSRF-POST + chsk URL.
    :ajax-get-or-ws-handshake-fn ; (fn [ring-req]) for Ring GET + chsk URL.
    :connected-uids ; Watchable, read-only (atom {:ws #{_} :ajax #{_} :any #{_}}).

  Common options:
    :user-id-fn        ; (fn [ring-req]) -> unique user-id for server>user push.
    :csrf-token-fn     ; (fn [ring-req]) -> CSRF token for Ajax POSTs.
    :handshake-data-fn ; (fn [ring-req]) -> arb user data to append to handshake evs.
    :send-buf-ms-ajax  ; [2]
    :send-buf-ms-ws    ; [2]
    :ws-conn-gc-ms     ; Should be > client's :ws-kalive-ms
    :packer            ; :edn (default), or an IPacker implementation (experimental).

  [1] e.g. `taoensso.sente.server-adapters.http-kit/http-kit-adapter` or
           `taoensso.sente.server-adapters.immutant/immutant-adapter`.
      You must have the necessary web-server dependency in your project.clj and
      the necessary entry in your namespace's `ns` form.

  [2] Optimization to allow transparent batching of rapidly-triggered
      server>user pushes. This is esp. important for Ajax clients which use a
      (slow) reconnecting poller. Actual event dispatch may occur <= given ms
      after send call (larger values => larger batch windows)."

  ;; :lp-conn-gc-ms ; Should be > client's :lp-timeout-ms ; TODO Maybe later

  [web-server-adapter ; Actually a server-ch-adapter, but that may be confusing
   & [{:keys [recv-buf-or-n send-buf-ms-ajax send-buf-ms-ws
              ws-conn-gc-ms #_lp-conn-gc-ms
              user-id-fn csrf-token-fn handshake-data-fn packer]
       :or   {recv-buf-or-n (async/sliding-buffer 1000)
              send-buf-ms-ajax 100
              send-buf-ms-ws   30
              ws-conn-gc-ms    (enc/ms :secs 40) ; > Client's :ws-kalive-ms
              ;; lp-conn-gc-ms (enc/ms :secs 25) ; > Client's :lp-timeout-ms
              user-id-fn    (fn [ring-req] (get-in ring-req [:session :uid]))
              csrf-token-fn (fn [ring-req]
                              (or (get-in ring-req [:session :csrf-token])
                                  (get-in ring-req [:session :ring.middleware.anti-forgery/anti-forgery-token])
                                  (get-in ring-req [:session "__anti-forgery-token"])))
              handshake-data-fn (fn [ring-req] nil)
              packer :edn}}]]

  (have? enc/pos-int? send-buf-ms-ajax send-buf-ms-ws)
  (have? #(satisfies? interfaces/IServerChanAdapter %) web-server-adapter)

  (let [packer  (coerce-packer packer)
        ch-recv (chan recv-buf-or-n)
        conns_  (atom {:ws   {} ; {<uid> {<client-id> <server-ch>}}
                       :ajax {} ; {<uid> {<client-id> [<?server-ch> <udt-last-connected>]}}
                       })

        connected-uids_   (atom {:ws #{} :ajax #{} :any #{}})
        send-buffers_     (atom {:ws  {} :ajax  {}}) ; {<uid> [<buffered-evs> <#{ev-uuids}>]}
        last-ws-msg-udts_ (atom {}) ; {<client-id> <udt>}, used for ws conn gc

        user-id-fn
        (fn [ring-req client-id]
          ;; Allow uid to depend (in part or whole) on client-id. Be cautious
          ;; of security implications.
          (or (user-id-fn (assoc ring-req :client-id client-id)) ::nil-uid))

        connect-uid!
        (fn [type uid] {:pre [(have? uid)]}
          (let [newly-connected?
                (swap-in! connected-uids_ []
                  (fn [{:keys [ws ajax any] :as old-m}]
                    (let [new-m
                          (case type
                            :ws   {:ws (conj ws uid) :ajax ajax            :any (conj any uid)}
                            :ajax {:ws ws            :ajax (conj ajax uid) :any (conj any uid)})]
                      (swapped new-m
                        (let [old-any (:any old-m)
                              new-any (:any new-m)]
                          (when (and (not (contains? old-any uid))
                                          (contains? new-any uid))
                            :newly-connected))))))]
            newly-connected?))

        upd-connected-uid! ; Useful for atomic disconnects
        (fn [uid] {:pre [(have? uid)]}
          (let [newly-disconnected?
                (swap-in! connected-uids_ []
                  (fn [{:keys [ws ajax any] :as old-m}]
                    (let [conns' @conns_
                          any-ws-clients?   (contains? (:ws   conns') uid)
                          any-ajax-clients? (contains? (:ajax conns') uid)
                          any-clients?      (or any-ws-clients?
                                                any-ajax-clients?)
                          new-m
                          {:ws   (if any-ws-clients?   (conj ws   uid) (disj ws   uid))
                           :ajax (if any-ajax-clients? (conj ajax uid) (disj ajax uid))
                           :any  (if any-clients?      (conj any  uid) (disj any  uid))}]
                      (swapped new-m
                        (let [old-any (:any old-m)
                              new-any (:any new-m)]
                          (when (and      (contains? old-any uid)
                                     (not (contains? new-any uid)))
                            :newly-disconnected))))))]
            newly-disconnected?))

        send-fn ; server>user (by uid) push
        (fn [user-id ev & [{:as opts :keys [flush?]}]]
          (let [uid     (if (= user-id :sente/all-users-without-uid) ::nil-uid user-id)
                _       (tracef "Chsk send: (->uid %s) %s" uid ev)
                _       (assert uid
                          (str "Support for sending to `nil` user-ids has been REMOVED. "
                               "Please send to `:sente/all-users-without-uid` instead."))
                _       (assert-event ev)
                ev-uuid (enc/uuid-str)

                flush-buffer!
                (fn [type]
                  (when-let
                      [pulled
                       (swap-in! send-buffers_ [type]
                         (fn [m]
                           ;; Don't actually flush unless the event buffered
                           ;; with _this_ send call is still buffered (awaiting
                           ;; flush). This means that we'll have many (go
                           ;; block) buffer flush calls that'll noop. They're
                           ;;  cheap, and this approach is preferable to
                           ;; alternatives like flush workers.
                           (let [[_ ev-uuids] (get m uid)]
                             (if (contains? ev-uuids ev-uuid)
                               (swapped (dissoc m uid)
                                        (get    m uid))
                               (swapped m nil)))))]
                    (let [[buffered-evs ev-uuids] pulled]
                      (have? vector? buffered-evs)
                      (have? set?    ev-uuids)

                      (let [packer-metas         (mapv meta buffered-evs)
                            combined-packer-meta (reduce merge {} packer-metas)
                            buffered-evs-ppstr   (pack packer
                                                   combined-packer-meta
                                                   buffered-evs)]
                        (tracef "buffered-evs-ppstr: %s (with meta %s)"
                          buffered-evs-ppstr combined-packer-meta)
                        (case type
                          :ws   (send-buffered-server-evs>ws-clients!   conns_
                                  uid buffered-evs-ppstr)
                          :ajax (send-buffered-server-evs>ajax-clients! conns_
                                  uid buffered-evs-ppstr))))))]

            (if (= ev [:chsk/close]) ; Currently undocumented
              (do
                (debugf "Chsk closing (client may reconnect): %s" uid)
                (when flush?
                  (doseq [type [:ws :ajax]]
                    (flush-buffer! type)))

                (doseq [server-ch (vals (get-in @conns_ [:ws uid]))]
                  (interfaces/sch-close! server-ch))

                (doseq [[?server-ch _] (vals (get-in @conns_ [:ajax uid]))]
                  (when-let [server-ch ?server-ch]
                    (interfaces/sch-close! server-ch))))

              (do
                ;; Buffer event
                (doseq [type [:ws :ajax]]
                  (swap-in! send-buffers_ [type uid]
                    (fn [?v]
                      (if-not ?v
                        [[ev] #{ev-uuid}]
                        (let [[buffered-evs ev-uuids] ?v]
                          [(conj buffered-evs ev)
                           (conj ev-uuids     ev-uuid)])))))

                ;;; Flush event buffers after relevant timeouts:
                ;; * May actually flush earlier due to another timeout.
                ;; * We send to _all_ of a uid's connections.
                ;; * Broadcasting is possible but I'd suggest doing it rarely, and
                ;;   only to users we know/expect are actually online.
                (go (when-not flush? (<! (async/timeout send-buf-ms-ws)))
                    (flush-buffer! :ws))
                (go (when-not flush? (<! (async/timeout send-buf-ms-ajax)))
                    (flush-buffer! :ajax)))))

          ;; Server-side send is async so nothing useful to return (currently
          ;; undefined):
          nil)

        ev-msg-const {:ch-recv        ch-recv
                      :send-fn        send-fn
                      :connected-uids connected-uids_}]

    {:ch-recv        ch-recv
     :send-fn        send-fn
     :connected-uids connected-uids_

     :ajax-post-fn ; Does not participate in `conns_` (has specific req->resp)
     (fn [ring-req]
       (interfaces/ring-req->server-ch-resp web-server-adapter ring-req
         {:on-open
          (fn [server-ch]
            (let [params        (get ring-req :params)
                  ppstr         (get params   :ppstr)
                  client-id     (get params   :client-id)
                  [clj has-cb?] (unpack packer ppstr)]

              (put-server-event-msg>ch-recv! ch-recv
                (merge ev-msg-const
                  {;; Note that the client-id is provided here just for the
                   ;; user's convenience. non-lp-POSTs don't actually need a
                   ;; client-id for Sente's own implementation:
                   :client-id client-id #_"unnecessary-for-non-lp-POSTs"

                   :ring-req  ring-req
                   :event     clj
                   :uid       (user-id-fn ring-req client-id)
                   :?reply-fn
                   (when has-cb?
                     (fn reply-fn [resp-clj] ; Any clj form
                       (tracef "Chsk send (ajax reply): %s" resp-clj)
                       ;; true iff apparent success:
                       (interfaces/-sch-send! server-ch
                         (pack packer (meta resp-clj) resp-clj)
                         :close-after-send)))}))

              (when-not has-cb?
                (tracef "Chsk send (ajax reply): dummy-cb-200")
                (interfaces/-sch-send! server-ch
                  (pack packer nil :chsk/dummy-cb-200)
                  :close-after-send))))}))

     :ajax-get-or-ws-handshake-fn ; Ajax handshake/poll, or WebSocket handshake
     (fn [ring-req]
       (let [csrf-token (csrf-token-fn ring-req)
             params     (get ring-req :params)
             client-id  (get params   :client-id)
             uid        (user-id-fn ring-req client-id)
             websocket? (:websocket? ring-req)
             sch-uuid_  (delay (enc/uuid-str 6))

             receive-event-msg! ; Partial
             (fn [event & [?reply-fn]]
               (put-server-event-msg>ch-recv! ch-recv
                 (merge ev-msg-const
                   {:client-id client-id
                    :ring-req  ring-req
                    :event     event
                    :?reply-fn ?reply-fn
                    :uid       uid})))

             handshake!
             (fn [server-ch]
               (tracef "Handshake!")
               (let [?handshake-data (handshake-data-fn ring-req)
                     handshake-ev
                     (if-not (nil? ?handshake-data) ; Micro optimization
                       [:chsk/handshake [uid csrf-token ?handshake-data]]
                       [:chsk/handshake [uid csrf-token]])]
                 (interfaces/-sch-send! server-ch
                   (pack packer nil handshake-ev)
                   (not websocket?))))]

         (if (str/blank? client-id)
           (let [err-msg "Client's Ring request doesn't have a client id. Does your server have the necessary keyword Ring middleware (`wrap-params` & `wrap-keyword-params`)?"]
             (errorf (str err-msg ": %s") ring-req)
             (throw (ex-info err-msg {:ring-req ring-req})))

           (interfaces/ring-req->server-ch-resp web-server-adapter ring-req
             {:on-open
              (fn [server-ch]
                (if websocket?
                  (do ; WebSocket handshake

                    (tracef "New WebSocket channel: %s (%s)"
                      uid (str server-ch)) ; _Must_ call `str` on server-ch
                    (reset-in! conns_ [:ws uid client-id] server-ch)
                    (when (connect-uid! :ws uid)
                      (receive-event-msg! [:chsk/uidport-open]))
                    (handshake! server-ch)

                    ;; Start ws conn gc loop
                    ;; Sudden abnormal disconnects (e.g. enabling airplane
                    ;; mode) prevent the conn from firing the normal
                    ;; :on-close event until only much later (determined by
                    ;; TCP settings).
                    (swap! last-ws-msg-udts_ (fn [m] (assoc m client-id (enc/now-udt))))
                    (when-let [ms ws-conn-gc-ms]
                      (go-loop []
                        (when-let [last-ws-msg-udt* (get @last-ws-msg-udts_ client-id)]
                          (<! (async/timeout ms))
                          (when-let [last-ws-msg-udt (get @last-ws-msg-udts_ client-id)]
                            (if (= last-ws-msg-udt last-ws-msg-udt*)
                              ;; No activity since last timeout => conn dead
                              (interfaces/sch-close! server-ch)
                              (recur)))))))

                  ;; Ajax handshake/poll connection:
                  (let [initial-conn-from-client?
                        (swap-in! conns_ [:ajax uid client-id]
                          (fn [?v] (swapped [server-ch (enc/now-udt)] (nil? ?v))))

                        handshake? (or initial-conn-from-client?
                                       (:handshake? params))]

                    (when (connect-uid! :ajax uid)
                      (receive-event-msg! [:chsk/uidport-open]))

                    (when handshake?
                      ;; Client will immediately repoll
                      (handshake! server-ch))

                    ;; For #150, #159
                    ;; Help clean up timed-out lp conns; http-kit doesn't
                    ;; need this but other servers could possibly benefit.
                    #_(when-let [ms lp-conn-gc-ms]
                      (go
                        (<! (async/timeout ms)) ; Default 25s gc vs 20s tout
                        (when-let [closed? (interfaces/sch-close! server-ch)]
                          #_(warnf "GC'ed an open lp conn: %s/%s" client-id @sch-uuid_)))))))

              :on-msg ; Only for WebSockets
              (fn [server-ch req-ppstr]
                (swap! last-ws-msg-udts_ (fn [m] (assoc m client-id (enc/now-udt))))
                (let [[clj ?cb-uuid] (unpack packer req-ppstr)]
                  (receive-event-msg! clj ; Should be ev
                    (when ?cb-uuid
                      (fn reply-fn [resp-clj] ; Any clj form
                        (tracef "Chsk send (ws reply): %s" resp-clj)
                        ;; true iff apparent success:
                        (interfaces/-sch-send! server-ch
                          (pack packer (meta resp-clj) resp-clj ?cb-uuid)
                          (not :close-after-send)))))))

              :on-close ; We rely on `on-close` to trigger for _every_ conn!
              (fn [server-ch status]
                ;; `status` is currently unused; its form varies depending on
                ;; the underlying web server

                (if websocket?
                  (do ; WebSocket close
                    (swap! last-ws-msg-udts_ (fn [m] (dissoc m client-id)))
                    (swap-in! conns_ [:ws uid]
                      (fn [?m]
                        (let [new-m (dissoc ?m client-id)]
                          (if (empty? new-m) :swap/dissoc new-m))))

                    ;; (when (upd-connected-uid! uid)
                    ;;   (receive-event-msg! [:chsk/uidport-close]))

                    (go
                      ;; Allow some time for possible reconnects (sole window
                      ;; refresh, etc.):
                      (<! (async/timeout 5000))

                      ;; Note different (simpler) semantics here than Ajax
                      ;; case since we don't have/want a `udt-disconnected` value.
                      ;; Ajax semantics: 'no reconnect since disconnect+5s'.
                      ;; WS semantics: 'still disconnected after disconnect+5s'.
                      ;;
                      (when (upd-connected-uid! uid)
                        (receive-event-msg! [:chsk/uidport-close]))))

                  ;; Ajax close
                  (let [udt-disconnected (enc/now-udt)]
                    (swap-in! conns_ [:ajax uid client-id]
                      (fn [[server-ch udt-last-connected]] [nil udt-last-connected]))

                    (go
                      ;; Allow some time for possible poller reconnects:
                      (<! (async/timeout 5000))
                      (let [disconnected?
                            (swap-in! conns_ [:ajax uid]
                              (fn [?m]
                                (let [[_ ?udt-last-connected] (get ?m client-id)
                                      disconnected?
                                      (and ?udt-last-connected ; Not yet gc'd
                                        (>= udt-disconnected
                                            ?udt-last-connected))]
                                  (if-not disconnected?
                                    (swapped ?m (not :disconnected))
                                    (let [new-m (dissoc ?m client-id)]
                                      (swapped
                                        (if (empty? new-m) :swap/dissoc new-m)
                                        :disconnected))))))]

                        (when disconnected?
                          (when (upd-connected-uid! uid)
                            (receive-event-msg! [:chsk/uidport-close]))))))))}))))}))

(defn- send-buffered-server-evs>ws-clients!
  "Actually pushes buffered events (as packed-str) to all uid's WebSocket conns."
  [conns_ uid buffered-evs-pstr]
  (tracef "send-buffered-server-evs>ws-clients!: %s" buffered-evs-pstr)
  (doseq [server-ch (vals (get-in @conns_ [:ws uid]))]
    (interfaces/-sch-send! server-ch buffered-evs-pstr (not :close-after-send))))

(defn- send-buffered-server-evs>ajax-clients!
  "Actually pushes buffered events (as packed-str) to all uid's Ajax conns.
  Allows some time for possible Ajax poller reconnects."
  [conns_ uid buffered-evs-pstr & [{:keys [nmax-attempts ms-base ms-rand]
                                    ;; <= 7 attempts at ~135ms ea = 945ms
                                    :or   {nmax-attempts 7
                                           ms-base       90
                                           ms-rand       90}}]]
  (comment (* 7 (+ 90 (/ 90 2.0))))
  (tracef "send-buffered-server-evs>ajax-clients!: %s" buffered-evs-pstr)
  (let [;; All connected/possibly-reconnecting client uuids:
        client-ids-unsatisfied (keys (get-in @conns_ [:ajax uid]))]
    (when-not (empty? client-ids-unsatisfied)
      ;; (tracef "client-ids-unsatisfied: %s" client-ids-unsatisfied)
      (go-loop [n 0 client-ids-satisfied #{}]
        (let [?pulled ; nil or {<client-id> [<?server-ch> <udt-last-connected>]}
              (swap-in! conns_ [:ajax uid]
                (fn [m] ; {<client-id> [<?server-ch> <udt-last-connected>]}
                  (let [ks-to-pull (remove client-ids-satisfied (keys m))]
                    ;; (tracef "ks-to-pull: %s" ks-to-pull)
                    (if (empty? ks-to-pull)
                      (swapped m nil)
                      (swapped
                        (reduce
                          (fn [m k]
                            (let [[?server-ch udt-last-connected] (get m k)]
                              (assoc m k [nil udt-last-connected])))
                          m ks-to-pull)
                        (select-keys m ks-to-pull))))))]
          (have? [:or nil? map?] ?pulled)
          (let [?newly-satisfied
                (when ?pulled
                  (reduce-kv
                   (fn [s client-id [?server-ch _]]
                     (if (or (nil? ?server-ch)
                             ;; server-ch may have closed already (`send!` will noop):
                             (not (interfaces/-sch-send! ?server-ch buffered-evs-pstr
                                    :close-after-send)))
                       s
                       (conj s client-id))) #{} ?pulled))
                now-satisfied (into client-ids-satisfied ?newly-satisfied)]
            ;; (tracef "now-satisfied: %s" now-satisfied)
            (when (and (< n nmax-attempts)
                       (some (complement now-satisfied) client-ids-unsatisfied))
              ;; Allow some time for possible poller reconnects:
              (<! (async/timeout (+ ms-base (rand-int ms-rand))))
              (recur (inc n) now-satisfied))))))))

;;;; Client API

#?(:cljs (def ajax-lite "Alias of `taoensso.encore/ajax-lite`" enc/ajax-lite))
#?(:cljs
   (defprotocol IChSocket
     (-chsk-connect! [chsk])
     (-chsk-disconnect! [chsk reconn?])
     (-chsk-reconnect! [chsk])
     (-chsk-send! [chsk ev opts])))

#?(:cljs
   (do
     (defn chsk-connect! [chsk] (-chsk-connect! chsk))
     (defn chsk-destroy! "Deprecated" [chsk] (-chsk-disconnect! chsk false))
     (defn chsk-disconnect! [chsk] (-chsk-disconnect! chsk false))
     (defn chsk-reconnect! "Useful for reauthenticating after login/logout, etc."
       [chsk] (-chsk-reconnect! chsk))))

#?(:cljs
   (defn chsk-send!
     "Sends `[ev-id ev-?data :as event]`, returns true on apparent success."
     ([chsk ev] (chsk-send! chsk ev {}))
     ([chsk ev ?timeout-ms ?cb] (chsk-send! chsk ev {:timeout-ms ?timeout-ms
                                                     :cb         ?cb}))
     ([chsk ev opts]
      (tracef "Chsk send: (%s) %s" (assoc opts :cb (boolean (:cb opts))) ev)
      (-chsk-send! chsk ev opts))))

#?(:cljs
   (defn- chsk-send->closed! [?cb-fn]
     (warnf "Chsk send against closed chsk.")
     (when ?cb-fn (?cb-fn :chsk/closed))
     false))

#?(:cljs
   (defn- assert-send-args [x ?timeout-ms ?cb]
     (assert-event x)
     (assert (or (and (nil? ?timeout-ms) (nil? ?cb))
                 (and (enc/nneg-int? ?timeout-ms)))
             (str "cb requires a timeout; timeout-ms should be a +ive integer: " ?timeout-ms))
     (assert (or (nil? ?cb) (ifn? ?cb) (enc/chan? ?cb))
             (str "cb should be nil, an ifn, or a channel: " (type ?cb)))))

#?(:cljs
   (defn- pull-unused-cb-fn! [cbs-waiting_ ?cb-uuid]
     (when-let [cb-uuid ?cb-uuid]
       (swap-in! cbs-waiting_ [cb-uuid]
                 (fn [?f] (swapped :swap/dissoc ?f))))))

#?(:cljs
   (defn- merge>chsk-state! [{:keys [chs state_] :as chsk} merge-state]
     (let [[old-state new-state]
           (swap-in! state_ []
                     (fn [old-state]
                       (let [new-state (merge old-state merge-state)

                             ;; Is this a reasonable way of helping client distinguish
                             ;; cause of an auto reconnect? Didn't give it much
                             ;; thought...
                             requested-reconnect?
                             (and (:requested-reconnect-pending? old-state)
                                  (do (:open? new-state))
                                  (not (:open? old-state)))

                             new-state
                             (if requested-reconnect?
                               (-> new-state
                                   (dissoc :requested-reconnect-pending?)
                                   (assoc :requested-reconnect? true))
                               (dissoc new-state :requested-reconnect?))]

                         (swapped new-state [old-state new-state]))))]

       (when (not= old-state new-state)
         ;; (debugf "Chsk state change: %s" new-state)
         (put! (:state chs) [:chsk/state new-state])
         new-state))))

#?(:cljs
   (defn- cb-chan-as-fn
     "Experimental, undocumented. Allows a core.async channel to be provided
     instead of a cb-fn. The channel will receive values of form
     [<event-id>.cb <reply>]."
     [?cb ev]
     (if (or (nil? ?cb) (ifn? ?cb))
       ?cb
       (do
         (have? enc/chan? ?cb)
         (assert-event ev)
         (let [[ev-id _] ev
               cb-ch ?cb]
           (fn [reply]
             (put! cb-ch
                   [(keyword (str (enc/fq-name ev-id) ".cb"))
                    reply])))))))

#?(:cljs
   (defn- receive-buffered-evs! [chs clj]
     (tracef "receive-buffered-evs!: %s" clj)
     (let [buffered-evs (have vector? clj)]
       (doseq [ev buffered-evs]
         (assert-event ev)
         ;; Server shouldn't send :chsk/ events to clients:
         (let [[id ?data] ev] (have? #(not= % "chsk") (namespace id)))
         (put! (:<server chs) ev)))))

#?(:cljs
   (defn- handle-when-handshake! [chsk-type chsk clj]
     (have? [:el #{:ws :ajax}] chsk-type)
     (let [handshake? (and (vector? clj) ; Nb clj may be callback reply
                           (= (first clj) :chsk/handshake))]

       (tracef "handle-when-handshake (%s): %s"
               (if handshake? :handshake :non-handshake) clj)

       (when handshake?
         (let [[_ [?uid ?csrf-token ?handshake-data]] clj
               {:keys [chs ever-opened?_]} chsk
               first-handshake? (compare-and-set! ever-opened?_ false true)
               new-state
               {:type           chsk-type ; :auto -> e/o #{:ws :ajax}, etc.
                :open?          true
                :ever-opened?   true
                :uid            ?uid
                :csrf-token     ?csrf-token
                :handshake-data ?handshake-data
                :first-open?    first-handshake?}

               handshake-ev
               [:chsk/handshake
                [?uid ?csrf-token ?handshake-data first-handshake?]]]

           (assert-event handshake-ev)
           (when (str/blank? ?csrf-token)
             (warnf "SECURITY WARNING: no CSRF token available for use by Sente"))

           (merge>chsk-state! chsk new-state)
           (put! (:internal chs) handshake-ev)

           :handled)))))

#?(:cljs
   (defrecord ChWebSocket
     ;; WebSocket-only IChSocket implementation
     ;; Handles (re)connections, keep-alives, cbs, etc.

     [client-id chs params packer url
      state_ ; {:type _ :open? _ :uid _ :csrf-token _}
      cbs-waiting_ ; {<cb-uuid> <fn> ...}
      socket_ kalive-ms kalive-timer_ kalive-due?_
      backoff-ms-fn ; (fn [nattempt]) -> msecs
      active-retry-id_ retry-count_ ever-opened?_
      err-fn]

     IChSocket
     (-chsk-disconnect! [chsk reconn?]
       (reset! active-retry-id_ "_disable-auto-retry")
       (when-let [t @kalive-timer_] (.clearInterval js/window t))
       (if reconn?
         (merge>chsk-state! chsk {:open? false :requested-reconnect-pending? true})
         (merge>chsk-state! chsk {:open? false}))
       (when-let [s @socket_] (.close s 1000 "CLOSE_NORMAL")))

     (-chsk-reconnect! [chsk]
       (-chsk-disconnect! chsk :reconn)
       (-chsk-connect! chsk))

     (-chsk-send! [chsk ev opts]
       (let [{?timeout-ms :timeout-ms ?cb :cb :keys [flush?]} opts
             _ (assert-send-args ev ?timeout-ms ?cb)
             ?cb-fn (cb-chan-as-fn ?cb ev)]
         (if-not (:open? @state_) ; Definitely closed
           (chsk-send->closed! ?cb-fn)

           ;; TODO Buffer before sending (but honor `:flush?`)
           (let [?cb-uuid (when ?cb-fn (enc/uuid-str 6))
                 ppstr (pack packer (meta ev) ev ?cb-uuid)]

             (when-let [cb-uuid ?cb-uuid]
               (reset-in! cbs-waiting_ [cb-uuid] (have ?cb-fn))
               (when-let [timeout-ms ?timeout-ms]
                 (go (<! (async/timeout timeout-ms))
                     (when-let [cb-fn* (pull-unused-cb-fn! cbs-waiting_ ?cb-uuid)]
                       (cb-fn* :chsk/timeout)))))

             (try
               (.send @socket_ ppstr)
               (reset! kalive-due?_ false)
               :apparent-success
               (catch js/Error e
                 (errorf e "Chsk send error")
                 (when-let [cb-uuid ?cb-uuid]
                   (let [cb-fn* (or (pull-unused-cb-fn! cbs-waiting_ cb-uuid)
                                    (have ?cb-fn))]
                     (cb-fn* :chsk/error)))
                 false))))))

     (-chsk-connect! [chsk]
       (when-let [WebSocket (or (enc/oget js/window "WebSocket")
                                (enc/oget js/window "MozWebSocket"))]
         (let [retry-id (enc/uuid-str)
               connect-fn
               (fn connect-fn []
                 (let [retry-fn
                       (fn []
                         (when (= @active-retry-id_ retry-id)
                           (let [retry-count* (swap! retry-count_ inc)
                                 backoff-ms (backoff-ms-fn retry-count*)]
                             (.clearInterval js/window @kalive-timer_)
                             (warnf "Chsk is closed: will try reconnect (%s)" retry-count*)
                             (.setTimeout js/window connect-fn backoff-ms))))

                       ?socket
                       (try
                         (WebSocket.
                           (enc/merge-url-with-query-string url
                                                            (merge params ; 1st (don't clobber impl.):
                                                                   {:client-id client-id})))
                         (catch js/Error e
                           (errorf e "WebSocket js/Error")
                           nil))]

                   (if-not ?socket
                     (retry-fn) ; Couldn't even get a socket

                     (reset! socket_
                             (doto ?socket
                               (aset "onerror"
                                     (fn [ws-ev]
                                       (errorf "WebSocket error: %s" ws-ev)
                                       ;; Experimental, for #214;
                                       (put! (:internal chs) [:chsk/ws-error ws-ev])
                                       (when-let [ef err-fn] (ef chsk))
                                       nil))

                               (aset "onmessage" ; Nb receives both push & cb evs!
                                     (fn [ws-ev]
                                       (let [;; Nb may or may NOT satisfy `event?` since we also
                                             ;; receive cb replies here! This is actually why
                                             ;; we prefix our pstrs to indicate whether they're
                                             ;; wrapped or not.
                                             ppstr (enc/oget ws-ev "data")
                                             [clj ?cb-uuid] (unpack packer ppstr)]
                                         ;; (assert-event clj) ;; NO!
                                         (or
                                           (and (handle-when-handshake! :ws chsk clj)
                                                (reset! retry-count_ 0))
                                           (if-let [cb-uuid ?cb-uuid]
                                             (if-let [cb-fn (pull-unused-cb-fn! cbs-waiting_
                                                                                cb-uuid)]
                                               (cb-fn clj)
                                               (warnf "Cb reply w/o local cb-fn: %s" clj))
                                             (let [buffered-evs clj]
                                               (receive-buffered-evs! chs buffered-evs)))))))

                               (aset "onopen"
                                     (fn [_ws-ev]
                                       (reset! kalive-timer_
                                               (.setInterval js/window
                                                             (fn []
                                                               (when @kalive-due?_ ; Don't ping unnecessarily
                                                                 (chsk-send! chsk [:chsk/ws-ping]))
                                                               (reset! kalive-due?_ true))
                                                             kalive-ms))
                                       ;; NO, better for server to send a handshake!:
                                       ;; (merge>chsk-state! chsk {:open? true})
                                       ))

                               ;; Fires repeatedly (on each connection attempt) while
                               ;; server is down:
                               (aset "onclose"
                                     (fn [ws-ev]
                                       (let [code (enc/oget ws-ev "code")
                                             reason (enc/oget ws-ev "reason")
                                             clean? (enc/oget ws-ev "wasClean")]

                                         ;; Firefox calls "onclose" while unloading,
                                         ;; Ref. http://goo.gl/G5BYbn:
                                         (if clean?
                                           (debugf "Clean WebSocket close, will not attempt reconnect")
                                           (do
                                             (merge>chsk-state! chsk {:open? false})
                                             (retry-fn)))))))))))]

           (reset! active-retry-id_ retry-id)
           (reset! retry-count_ 0)
           (connect-fn)
           chsk)))))

#?(:cljs
   (defn- new-ChWebSocket [opts]
     (map->ChWebSocket
       (merge
         {:state_           (atom {:type :ws :open? false})
          :cbs-waiting_     (atom {})
          :socket_          (atom nil)
          :kalive-timer_    (atom nil)
          :kalive-due?_     (atom true)
          :active-retry-id_ (atom "_pending")
          :retry-count_     (atom 0)
          :ever-opened?_    (atom false)}
         opts))))

#?(:cljs
   (defrecord ChAjaxSocket
     ;; Ajax-only IChSocket implementation
     ;; Handles (re)polling, etc.

     [client-id chs params packer url state_
      timeout-ms ajax-opts curr-xhr_
      active-retry-id_
      backoff-ms-fn
      ever-opened?_]

     IChSocket
     (-chsk-disconnect! [chsk reconn?]
       (reset! active-retry-id_ "_disable-auto-retry")
       (if reconn?
         (merge>chsk-state! chsk {:open? false :requested-reconnect-pending? true})
         (merge>chsk-state! chsk {:open? false}))
       (when-let [x @curr-xhr_] (.abort x)))

     (-chsk-reconnect! [chsk]
       (-chsk-disconnect! chsk :reconn)
       (-chsk-connect! chsk))

     (-chsk-send! [chsk ev opts]
       (let [{?timeout-ms :timeout-ms ?cb :cb :keys [flush?]} opts
             _ (assert-send-args ev ?timeout-ms ?cb)
             ?cb-fn (cb-chan-as-fn ?cb ev)]
         (if-not (:open? @state_) ; Definitely closed
           (chsk-send->closed! ?cb-fn)

           ;; TODO Buffer before sending (but honor `:flush?`)
           (let [csrf-token (:csrf-token @state_)]
             (ajax-lite url
                        (merge ajax-opts
                               {:method    :post :timeout-ms ?timeout-ms
                                :resp-type :text ; We'll do our own pstr decoding
                                :headers
                                           (merge (:headers ajax-opts) ; 1st (don't clobber impl.):
                                                  {:X-CSRF-Token csrf-token})

                                :params
                                           (let [ppstr (pack packer (meta ev) ev (when ?cb-fn :ajax-cb))]
                                             (merge params ; 1st (don't clobber impl.):
                                                    {:_          (enc/now-udt) ; Force uncached resp

                                                     ;; A duplicate of X-CSRF-Token for user's convenience and
                                                     ;; for back compatibility with earlier CSRF docs:
                                                     :csrf-token csrf-token

                                                     ;; Just for user's convenience here. non-lp-POSTs don't
                                                     ;; actually need a client-id for Sente's own implementation:
                                                     :client-id  client-id

                                                     :ppstr      ppstr}))})

                        (fn ajax-cb [{:keys [?error ?content]}]
                          (if ?error
                            (if (= ?error :timeout)
                              (when ?cb-fn (?cb-fn :chsk/timeout))
                              (do (merge>chsk-state! chsk {:open? false})
                                  (when ?cb-fn (?cb-fn :chsk/error))))

                            (let [content ?content
                                  resp-ppstr content
                                  [resp-clj _] (unpack packer resp-ppstr)]
                              (if ?cb-fn (?cb-fn resp-clj)
                                         (when (not= resp-clj :chsk/dummy-cb-200)
                                           (warnf "Cb reply w/o local cb-fn: %s" resp-clj)))
                              (merge>chsk-state! chsk {:open? true})))))

             :apparent-success))))

     (-chsk-connect! [chsk]
       (let [retry-id (enc/uuid-str)
             poll-fn ; async-poll-for-update-fn
             (fn poll-fn [retry-count]
               (tracef "async-poll-for-update!")
               (let [retry-fn
                     (fn []
                       (when (= @active-retry-id_ retry-id)
                         (let [retry-count* (inc retry-count)
                               backoff-ms (backoff-ms-fn retry-count*)]
                           (warnf "Chsk is closed: will try reconnect (%s)" retry-count*)
                           (.setTimeout js/window (fn [] (poll-fn retry-count*)) backoff-ms))))]

                 (reset! curr-xhr_
                         (ajax-lite url
                                    (merge ajax-opts
                                           {:method    :get :timeout-ms timeout-ms
                                            :resp-type :text ; Prefer to do our own pstr reading
                                            :params
                                                       (merge

                                                         ;; Note that user params here are actually POST params for
                                                         ;; convenience. Contrast: WebSocket params sent as query
                                                         ;; params since there's no other choice there.
                                                         params ; 1st (don't clobber impl.):

                                                         {:_         (enc/now-udt) ; Force uncached resp
                                                          :client-id client-id}

                                                         ;; A truthy :handshake? param will prompt server to
                                                         ;; reply immediately with a handshake response,
                                                         ;; letting us confirm that our client<->server comms
                                                         ;; are working:
                                                         (when-not (:open? @state_) {:handshake? true}))})

                                    (fn ajax-cb [{:keys [?error ?content]}]
                                      (if ?error
                                        (cond
                                          (= ?error :timeout) (poll-fn 0)
                                          ;; (= ?error :abort) ; Abort => intentional, not an error
                                          :else
                                          (do (merge>chsk-state! chsk {:open? false})
                                              (retry-fn)))

                                        ;; The Ajax long-poller is used only for events, never cbs:
                                        (let [content ?content
                                              ppstr content
                                              [clj _] (unpack packer ppstr)]
                                          (or
                                            (handle-when-handshake! :ajax chsk clj)
                                            ;; Actually poll for an application reply:
                                            (let [buffered-evs clj]
                                              (receive-buffered-evs! chs buffered-evs)
                                              (merge>chsk-state! chsk {:open? true})))

                                          (poll-fn 0))))))))]

         (reset! active-retry-id_ retry-id)
         (poll-fn 0)
         chsk))))

#?(:cljs
   (defn- new-ChAjaxSocket [opts]
     (map->ChAjaxSocket
       (merge
         {:state_           (atom {:type :ajax :open? false})
          :curr-xhr_        (atom nil)
          :active-retry-id_ (atom "_pending")
          :ever-opened?_    (atom false)}
         opts))))

#?(:cljs
   (defrecord ChAutoSocket
     ;; Dynamic WebSocket/Ajax IChSocket implementation
     ;; Wraps a swappable ChWebSocket/ChAjaxSocket

     [ws-chsk-opts ajax-chsk-opts
      state_ ; {:type _ :open? _ :uid _ :csrf-token _}
      impl_ ; ChWebSocket or ChAjaxSocket
      ]

     IChSocket
     (-chsk-disconnect! [chsk reconn?]
       (when-let [impl @impl_]
         (-chsk-disconnect! impl reconn?)))

     ;; Possibly reset impl type:
     (-chsk-reconnect! [chsk]
       (when-let [impl @impl_]
         (-chsk-disconnect! impl :reconn)
         (-chsk-connect! chsk)))

     (-chsk-send! [chsk ev opts]
       (if-let [impl @impl_]
         (-chsk-send! impl ev opts)
         (let [{?cb :cb} opts
               ?cb-fn (cb-chan-as-fn ?cb ev)]
           (chsk-send->closed! ?cb-fn))))

     (-chsk-connect! [chsk]
       (let [ajax-chsk-opts (assoc ajax-chsk-opts :state_ state_)
             ajax-conn! (fn [] (-chsk-connect! (new-ChAjaxSocket ajax-chsk-opts)))

             ws-err-fn ; Called on WebSocket's onerror
             (fn [impl]
               ;; Starting with something simple here as a proof of concept;
               ;; TODO Consider smarter downgrade/upgrade strategies here later
               (warnf "Permanently downgrading :auto chsk -> :ajax")
               (-chsk-disconnect! impl false)
               (reset! impl_ (ajax-conn!)))

             ws-chsk-opts (assoc ws-chsk-opts :state_ state_ :err-fn ws-err-fn)
             ws-conn! (fn [] (-chsk-connect! (new-ChWebSocket ws-chsk-opts)))]

         (reset! impl_ (or (ws-conn!) (ajax-conn!)))
         chsk))))

#?(:cljs
   (defn- new-ChAutoSocket [opts]
     (map->ChAutoSocket
       (merge
         {:state_ (atom {:type :auto :open? false})
          :impl_  (atom nil)}
         opts))))

#?(:cljs
   (defn- get-chsk-url [protocol host path type]
     (let [protocol (case type
                      :ajax protocol
                      :ws (if (= protocol "https:") "wss:" "ws:"))]
       (str protocol "//" (enc/path host path)))))

#?(:cljs
   (defn make-channel-socket-client!
     "Returns nil on failure, or a map with keys:
       :ch-recv ; core.async channel to receive `event-msg`s (internal or from
                ; clients). May `put!` (inject) arbitrary `event`s to this channel.
       :send-fn ; (fn [event & [?timeout-ms ?cb-fn]]) for client>server send.
       :state   ; Watchable, read-only (atom {:type _ :open? _ :uid _ :csrf-token _}).
       :chsk    ; IChSocket implementer. You can usu. ignore this.

     Common options:
       :type           ; e/o #{:auto :ws :ajax}. You'll usually want the default (:auto)
       :host           ; Server host (defaults to current page's host)
       :params         ; Map of any params to incl. in chsk Ring requests (handy
                       ; for application-level auth, etc.)
       :ws-kalive-ms   ; Ping to keep a WebSocket conn alive if no activity w/in
                       ; given number of milliseconds
       :lp-timeout-ms  ; Ping to keep a long-polling (Ajax) conn alive '' [1]
       :packer         ; :edn (default), or an IPacker implementation (experimental)
       :ajax-opts      ; Base opts map provided to `taoensso.encore/ajax-lite`
       :wrap-recv-evs? ; Should events from server be wrapped in [:chsk/recv _]?

     [1] If you're using Immutant and override the default :lp-timeout-ms,
         you'll need to provide the same timeout value to
         `taoensso.sente.server-adapters.immutant/make-immutant-adapter` and use
         the result of that function as the web server adapter to your
         server-side `make-channel-socket-server!`."

     [path &
      [{:keys [type host params recv-buf-or-n ws-kalive-ms lp-timeout-ms packer
               client-id ajax-opts wrap-recv-evs? backoff-ms-fn]
        :as   opts
        :or   {type           :auto
               recv-buf-or-n  (async/sliding-buffer 2048) ; Mostly for buffered-evs
               ws-kalive-ms   (enc/ms :secs 35) ; < Heroku 55s timeout
               lp-timeout-ms  (enc/ms :secs 20) ; < Heroku 30s timeout
               packer         :edn
               client-id      (or (:client-uuid opts) ; Backwards compatibility
                                  (enc/uuid-str))

               ;; TODO Deprecated. Default to false later, then eventually just
               ;; drop this option altogether? - here now for back compatibility:
               wrap-recv-evs? true

               backoff-ms-fn  enc/exp-backoff}}

       _deprecated-more-opts]]

     (have? [:in #{:ajax :ws :auto}] type)
     (have? enc/nblank-str? client-id)

     (when (not (nil? _deprecated-more-opts)) (warnf "`make-channel-socket-client!` fn signature CHANGED with Sente v0.10.0."))
     (when (contains? opts :lp-timeout) (warnf ":lp-timeout opt has CHANGED; please use :lp-timout-ms."))

     (let [packer (coerce-packer packer)

           win-loc (enc/get-win-loc)
           win-protocol (:protocol win-loc)
           host (or host (:host win-loc))
           path (or path (:pathname win-loc))

           [ws-url ajax-url]
           (if-let [f (:chsk-url-fn opts)] ; Deprecated
             [(f path win-loc :ws) (f path win-loc :ajax)]
             [(get-chsk-url win-protocol host path :ws)
              (get-chsk-url win-protocol host path :ajax)])

           private-chs
           {:internal (chan (async/sliding-buffer 10))
            :state    (chan (async/sliding-buffer 10))
            :<server  (chan (async/sliding-buffer 10))}

           common-chsk-opts
           {:client-id client-id
            :chs       private-chs
            :params    params
            :packer    packer}

           ws-chsk-opts
           (merge common-chsk-opts
                  {:url           ws-url
                   :kalive-ms     ws-kalive-ms
                   :backoff-ms-fn backoff-ms-fn})

           ajax-chsk-opts
           (merge common-chsk-opts
                  {:url           ajax-url
                   :timeout-ms    lp-timeout-ms
                   :ajax-opts     ajax-opts
                   :backoff-ms-fn backoff-ms-fn})

           auto-chsk-opts
           {:ws-chsk-opts   ws-chsk-opts
            :ajax-chsk-opts ajax-chsk-opts}

           ?chsk
           (-chsk-connect!
             (case type
               :ws (new-ChWebSocket ws-chsk-opts)
               :ajax (new-ChAjaxSocket ajax-chsk-opts)
               :auto (new-ChAutoSocket auto-chsk-opts)))]

       (if-let [chsk ?chsk]
         (let [send-fn (partial chsk-send! chsk)

               ;; TODO map< is deprecated, prefer transducers (needs clj 1.7+)

               ev-ch
               (async/merge
                 [(do (:internal private-chs))
                  (do (:state private-chs))
                  (let [<server-ch (:<server private-chs)]
                    (if wrap-recv-evs?
                      (async/map< (fn [ev] [:chsk/recv ev]) <server-ch)
                      <server-ch))]
                 recv-buf-or-n)

               ev-msg-ch
               (async/map<
                 ;; All client-side `event-msg`s go through this (allows client to
                 ;; inject arbitrary synthetic events into router for handling):
                 (fn ev->ev-msg [ev]
                   (let [[ev-id ev-?data :as ev] (as-event ev)]
                     {:ch-recv ev-ch
                      :send-fn send-fn
                      :state   (:state_ chsk)
                      :event   ev
                      :id      ev-id
                      :?data   ev-?data}))
                 ev-ch)]

           {:chsk    chsk
            :ch-recv ev-msg-ch ; Public `ev`s->`ev-msg`s ch
            :send-fn send-fn
            :state   (:state_ chsk)})

         (warnf "Failed to create channel socket")))))

;;;; Event-msg routers (handler loops)

(defn- -start-chsk-router!
  [server? ch-recv event-msg-handler opts]
  (let [{:keys [trace-evs? error-handler]} opts
        ch-ctrl (chan)]

    (go-loop []
      (let [[v p] (async/alts! [ch-recv ch-ctrl])
            stop? (enc/kw-identical? p  ch-ctrl)]

        (when-not stop?
          (let [{:as event-msg :keys [event]} v
                [_ ?error]
                (enc/catch-errors
                  (when trace-evs? (tracef "Pre-handler event: %s" event))
                  (event-msg-handler
                    (if server?
                      (have! server-event-msg? event-msg)
                      (have! client-event-msg? event-msg))))]

            (when-let [e ?error]
              (let [[_ ?error2]
                    (enc/catch-errors
                      (if-let [eh error-handler]
                        (error-handler e event-msg)
                        (errorf e "Chsk router `event-msg-handler` error: %s" event)))]
                (when-let [e2 ?error2]
                  (errorf e2 "Chsk router `error-handler` error: %s" event))))

            (recur)))))

    (fn stop! [] (async/close! ch-ctrl))))

(defn start-server-chsk-router!
  "Creates a go-loop to call `(event-msg-handler <server-event-msg>)` and
  log any errors. Returns a `(fn stop! [])`.

  For performance, you'll likely want your `event-msg-handler` fn to be
  non-blocking (at least for slow handling operations). Clojure offers
  a rich variety of tools here including futures, agents, core.async,
  etc.

  Advanced users may also prefer to write their own loop against `ch-recv`."
  [ch-recv event-msg-handler & [{:as opts :keys [trace-evs? error-handler]}]]
  (-start-chsk-router! :server ch-recv event-msg-handler opts))

(defn start-client-chsk-router!
  "Creates a go-loop to call `(event-msg-handler <client-event-msg>)` and
  log any errors. Returns a `(fn stop! [])`.

  For performance, you'll likely want your `event-msg-handler` fn to be
  non-blocking (at least for slow handling operations). Clojure offers
  a rich variety of tools here including futures, agents, core.async,
  etc.

  Advanced users may also prefer to write their own loop against `ch-recv`."
  [ch-recv event-msg-handler & [{:as opts :keys [trace-evs? error-handler]}]]
  (-start-chsk-router! (not :server) ch-recv event-msg-handler opts))

;;;; Platform aliases

(def event-msg? #?(:clj server-event-msg? :cljs client-event-msg?))

(def make-channel-socket!
  #?(:clj  make-channel-socket-server!
     :cljs make-channel-socket-client!))

(def start-chsk-router!
  #?(:clj  start-server-chsk-router!
     :cljs start-client-chsk-router!))

;;;; Deprecated

#?(:clj
   (defn start-chsk-router-loop!
     "DEPRECATED: Please use `start-chsk-router!` instead"
     [event-msg-handler ch-recv]
     (start-server-chsk-router! ch-recv
       ;; Old handler form: (fn [ev-msg ch-recv])
       (fn [ev-msg] (event-msg-handler ev-msg (:ch-recv ev-msg))))))

#?(:clj
   (defn start-chsk-router-loop!
     "DEPRECATED: Please use `start-chsk-router!` instead"
     [event-handler ch-recv]
     (start-client-chsk-router! ch-recv
       ;; Old handler form: (fn [ev ch-recv])
       (fn [ev-msg] (event-handler (:event ev-msg) (:ch-recv ev-msg))))))

(def set-logging-level! "DEPRECATED. Please use `timbre/set-level!` instead" timbre/set-level!)

#?(:cljs (def ajax-call "DEPRECATED: Please use `ajax-lite` instead" enc/ajax-lite))
#?(:cljs
   (def default-chsk-url-fn "DEPRECATED"
     (fn [path {:as location :keys [protocol host pathname]} websocket?]
       (let [protocol
             (if websocket?
               (if (= protocol "https:") "wss:" "ws:")
               protocol)]
         (str protocol "//" host (or path pathname))))))
