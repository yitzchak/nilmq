(in-package #:nilmq)

;;; The CHANNEL Socket Type
;;; https://rfc.zeromq.org/spec/52/#the-channel-socket-type

(defclass channel-socket (pair-socket)
  ())

(defmethod socket-type ((socket channel-socket))
  "CHANNEL")

(defmethod make-socket ((type (eql :channel)) &key (context *context*))
  (make-instance 'channel-socket :context context))

(defmethod process :around ((socket channel-socket) (peer peer) (object cons))
  (unless (cdr object)
    (call-next-method)))

(defmethod send :around ((socket pair-socket) (message cons))
  (unless (cdr message)
    (call-next-method)))
