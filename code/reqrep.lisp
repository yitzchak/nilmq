(in-package #:nilmq)

;;; The REQ Socket Type
;;; https://rfc.zeromq.org/spec/28/#the-req-socket-type

(defclass req-socket (round-robin-socket)
  ((active :accessor active
           :initform nil)
   (send-lock :accessor send-lock
              :initform (bt2:make-lock))
   (active-semaphore :accessor active-semaphore
                     :initform (bt2:make-semaphore))))

(defmethod socket-type ((socket req-socket))
  "REQ")

(defmethod make-socket ((type (eql :req)) &key (context *context*))
  (make-instance 'req-socket :context context))

(defmethod process ((socket req-socket) (peer peer) (object cons))
  (with-accessors ((active active)
                   (active-semaphore active-semaphore))
      socket
    (when (and (active socket)
               (eq peer (find-peer socket)))
      (enqueue (input-queue socket) (cdr (member-if (lambda (x) (zerop (length x))) object)))
      (setf active nil)
      (next-peer socket)
      #+(or)(bt2:signal-semaphore (active-semaphore socket)))))

(defmethod send ((socket req-socket) (message cons))
  (with-accessors ((active active)
                   (send-lock send-lock)
                   (active-semaphore active-semaphore))
      socket
    (bt2:with-lock-held (send-lock)
      (send (target (find-peer socket)) (cons #() message))
      (setf active t)
      #+(or)(bt2:wait-on-semaphore active-semaphore)))
  nil)

;;; The REP Socket Type
;;; https://rfc.zeromq.org/spec/28/#the-rep-socket-type

(defclass rep-socket (pool-socket)
  ((%address-queue :accessor address-queue
                   :initform (make-instance 'queue))))

(defmethod socket-type ((socket rep-socket))
  "REP")

(defmethod make-socket ((type (eql :rep)) &key (context *context*))
  (make-instance 'rep-socket :context context))

(defmethod process ((socket rep-socket) (peer peer) (object cons))
  (loop for (part . message) on object
        initially (setf (skip-read-p peer) t)
        collect part into address
        when (and (typep part sequence)
                  (zerop (length part)))
          do (enqueue (address-queue socket) (cons peer address))
             (enqueue (input-queue socket) message)))

(defmethod send ((socket rep-socket) (message cons))
  (destructuring-bind (peer . address)
      (dequeue (address-queue socket))
    (send peer (nconc address message))))

;;; The DEALER Socket Type
;;; https://rfc.zeromq.org/spec/28/#the-dealer-socket-type

(defclass dealer-socket (round-robin-socket)
  ())

(defmethod socket-type ((socket dealer-socket))
  "DEALER")

(defmethod make-socket ((type (eql :dealer)) &key (context *context*))
  (make-instance 'dealer-socket :context context))

(defmethod send ((socket dealer-socket) (message cons))
  (send (find-peek socket) message)
  (next-peer socket))

(defmethod process ((socket dealer-socket) (peer peer) (object cons))
  (enqueue (input-queue socket) object))

;;; The ROUTER Socket Type
;;; https://rfc.zeromq.org/spec/28/#the-router-socket-type

(defclass router-socket (address-socket)
  ())

(defmethod socket-type ((socket router-socket))
  "ROUTER")

(defmethod make-socket ((type (eql :router)) &key (context *context*))
  (make-instance 'router-socket :context context))

(defmethod send ((socket router-socket) (message cons))
  (let ((peer (find-peer socket (car message))))
    (when peer
      (send peer (cdr message)))))

(defmethod process ((socket router-socket) (peer peer) (object cons))
  (enqueue (input-queue socket) (cons (routing-id peer) object)))
