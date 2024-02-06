(in-package #:nilmq)

;;; The PUB Socket Type
;;; https://rfc.zeromq.org/spec/29/#the-pub-socket-type

(defclass pub-socket (pool-socket)
  ()
  (:default-initargs :input-queue (make-instance 'null-queue)))

(defmethod socket-type ((socket pub-socket))
  "PUB")

(defmethod make-socket ((type (eql :pub)) &key (context *context*))
  (make-instance 'pub-socket :context context))

(defmethod send ((socket pub-socket) (message cons))
  (map-peers socket
             (lambda (peer)
               (when (or (null (subscriptions peer))
                         (some (lambda (sub)
                                 (or (zerop (length sub))
                                     (not (mismatch sub (car message) :end1 (length sub)))))
                               (subscriptions peer)))
                 (send peer message)))))

;;; The XPUB Socket Type
;;; https://rfc.zeromq.org/spec/29/#the-xpub-socket-type

(defclass xpub-socket (pub-socket)
  ()
  (:default-initargs :input-queue (make-instance 'queue)))

(defmethod socket-type ((socket pub-socket))
  "XPUB")

(defmethod make-socket ((type (eql :xpub)) &key (context *context*))
  (make-instance 'xpub-socket :context context))

(defmethod process ((socket xpub-socket) (peer peer) (object cons))
  (enqueue (input-queue socket) object))

;;; The SUB Socket Type
;;; https://rfc.zeromq.org/spec/29/#the-sub-socket-type

(defclass sub-socket (pool-socket)
  ((%topics :accessor topics
            :initform nil)))

(defmethod socket-type ((socket sub-socket))
  "SUB")

(defmethod make-socket ((type (eql :sub)) &key (context *context*))
  (make-instance 'sub-socket :context context))

(defmethod subscribe ((socket sub-socket) topic)
  (push topic (topics socket))
  (let ((command (make-instance 'subscribe-command :data topic)))
    (map-peers socket
               (lambda (peer)
                 (send peer command)))))

(defmethod unsubscribe ((socket sub-socket) topic)
  (setf (topics socket) (delete topic (topics socket) :test #'equal :count 1))
  (let ((command (make-instance 'cancel-command :data topic)))
    (map-peers socket
               (lambda (peer)
                 (send peer command)))))

(defmethod add-peer :after ((socket sub-socket) peer)
  (loop for topic in (topics socket)
        do (send peer (make-instance 'subscribe-command :data topic))))

;;; The XSUB Socket Type
;;; https://rfc.zeromq.org/spec/29/#the-xsub-socket-type

(defclass xsub-socket (sub-socket)
  ((%topics :accessor topics
            :initform nil)))

(defmethod socket-type ((socket xsub-socket))
  "XSUB")

(defmethod make-socket ((type (eql :xsub)) &key (context *context*))
  (make-instance 'xsub-socket :context context))

(defmethod send ((socket xsub-socket) (message cons))
  (map-peers socket
             (lambda (peer)
               (send peer message))))
