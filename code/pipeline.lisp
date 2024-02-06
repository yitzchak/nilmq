(in-package #:nilmq)

;;; The PUSH Socket Type
;;; https://rfc.zeromq.org/spec/30/#the-push-socket-type

(defclass push-socket (round-robin-socket)
  ())

(defmethod socket-type ((socket push-socket))
  "PUSH")

(defmethod make-socket ((type (eql :push)) &key (context *context*))
  (make-instance 'push-socket :context context))

(defmethod send ((socket push-socket) (message cons))
  (send (find-peer socket) message)
  (next-peer socket))

;;; The PULL Socket Type
;;; https://rfc.zeromq.org/spec/30/#the-pull-socket-type

(defclass pull-socket (pool-socket)
  ())

(defmethod socket-type ((socket pull-socket))
  "PULL")

(defmethod make-socket ((type (eql :pull)) &key (context *context*))
  (make-instance 'pull-socket :context context))

(defmethod process ((socket pull-socket) (peer peer) (object cons))
  (enqueue (input-queue socket) object))
