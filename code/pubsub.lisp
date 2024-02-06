(in-package #:nilmq)

(defclass pub-socket (socket)
  ((%peers :accessor peers
           :initform nil))
  (:default-initargs :input-queue (make-instance 'null-queue)))

(defmethod socket-type ((socket pub-socket))
  "PUB")

(defmethod die :after ((socket pub-socket) peer)
  (setf (peers socket) (delete peer (peers socket))))

(defmethod start-peer :after ((socket pub-socket) peer)
  (push peer (peers socket)))

(defmethod make-socket ((type (eql :pub)) &key (context *context*))
  (let ((socket (make-instance 'pub-socket :context context)))
    (enqueue-task socket (lambda ()
                           (poll socket)
                           t))
    socket))

(defmethod send ((socket pub-socket) (message cons))
  (loop for peer in (peers socket)
        when (or (null (subscriptions peer))
                 (some (lambda (sub)
                         (or (zerop (length sub))
                             (not (mismatch sub (car message) :end1 (length sub)))))
                       (subscriptions peer)))
          do (send peer message)))

(defmethod poll :after ((socket pub-socket))
  (loop for peer in (peers socket)
        do (poll peer)))

(defclass xpub-socket (pub-socket)
  ()
  (:default-initargs :input-queue (make-instance 'queue)))

(defmethod socket-type ((socket pub-socket))
  "XPUB")

(defmethod make-socket ((type (eql :xpub)) &key (context *context*))
  (let ((socket (make-instance 'xpub-socket :context context)))
    (setf (thread socket)
          (bordeaux-threads:make-thread
           (lambda ()
             (loop (poll socket)))))
    socket))

(defmethod process ((socket xpub-socket) (peer peer) (object cons))
  (enqueue (input-queue socket) object))
