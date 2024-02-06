(in-package #:nilmq)

;;; The PAIR Socket Type
;;; https://rfc.zeromq.org/spec/31/#the-pair-socket-type

(defclass pair-socket (couple-socket)
  ())

(defmethod socket-type ((socket pair-socket))
  "PAIR")

(defmethod make-socket ((type (eql :pair)) &key (context *context*))
  (make-instance 'pair-socket :context context))

(defmethod process ((socket pair-socket) (peer peer) (object cons))
  (enqueue (input-queue socket) object))

(defmethod send ((socket pair-socket) (message cons))
  (map-peers socket
             (lambda (peer)
               (send peer message))))
