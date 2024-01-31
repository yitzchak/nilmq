(in-package #:nilmq)

(defclass pub-socket (nonblocking-socket)
  ((%connections :accessor connections
                 :initform nil))
  (:default-initargs :input-queue (make-instance 'null-queue)))

(defmethod socket-type ((socket pub-socket))
  "PUB")

(defmethod start-connection :after ((socket pub-socket) connection)
  (push connection (connections socket)))

(defmethod make-socket ((type (eql :pub)))
  (let ((socket (make-instance 'pub-socket)))
    (setf (thread socket)
          (bordeaux-threads:make-thread
           (lambda ()
             (loop (poll socket)))))
    socket))

(defmethod send ((socket pub-socket) (message cons))
  (loop for connection in (connections socket)
        when (some (lambda (sub)
                (not (mismatch sub (car message) :end1 (length sub))))
                   (subscriptions connection))
          do (send connection message)))

(defmethod poll :after ((socket pub-socket))
  (loop for connection in (connections socket)
        do (poll connection)))

(defclass xpub-socket (pub-socket)
  ()
  (:default-initargs :input-queue (make-instance 'queue)))

(defmethod socket-type ((socket pub-socket))
  "XPUB")

(defmethod make-socket ((type (eql :xpub)))
  (let ((socket (make-instance 'xpub-socket)))
    (setf (thread socket)
          (bordeaux-threads:make-thread
           (lambda ()
             (loop (poll socket)))))
    socket))

(defmethod process ((socket xpub-socket) (connection connection) (object cons))
  (enqueue (input-queue socket) object))
