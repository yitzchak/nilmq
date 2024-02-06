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
  (let ((socket (make-instance 'req-socket :context context)))
    (enqueue-task socket (lambda ()
                           (poll socket)
                           t))
    socket))

(defmethod start-peer :after ((socket req-socket) peer)
  (add-peer socket peer))

(defmethod poll :after ((socket req-socket))
  (map-peers socket #'poll))

(defmethod process ((socket req-socket) (peer peer) (object cons))
  (with-accessors ((active active)
                   (active-semaphore active-semaphore))
      socket
    (when (and (active socket)
               (eq peer (find-peer socket)))
      (enqueue (input-queue socket) (cdr (member-if (lambda (x) (zerop (length x))) object)))
      (bt2:signal-semaphore (active-semaphore socket)))))

(defmethod die :after ((socket req-socket) peer)
  (remove-peer socket peer))

(defmethod send ((socket req-socket) (message cons))
  (with-accessors ((active active)
                   (send-lock send-lock)
                   (active-semaphore active-semaphore))
      socket
    (bt2:with-lock-held (send-lock)
      (send (find-peer socket) (cons #() message))
      (setf active t)
      (bt2:wait-on-semaphore active-semaphore)
      (setf active nil)
      (next-peer socket)))
  nil)

;;; The REP Socket Type
;;; https://rfc.zeromq.org/spec/28/#the-rep-socket-type

(defclass rep-socket (address-socket)
  ((%output-queue :accessor output-queue
                  :initform (make-instance 'queue))
   (%peers :accessor peers
           :initform nil)))

(defmethod socket-type ((socket rep-socket))
  "REP")

(defmethod make-socket ((type (eql :rep)) &key (context *context*))
  (let ((socket (make-instance 'rep-socket :context context)))
    (enqueue-task socket (lambda ()
                           (poll socket)
                           t))
    socket))

(defmethod start-peer :after ((socket rep-socket) peer)
  (push peer (peers socket)))

(defmethod poll :after ((socket rep-socket))
  (mapc #'poll (peers socket)))

(defmethod process ((socket rep-socket) (peer peer) (object cons))
  (let ((delimiter (member-if (lambda (x) (zerop (length x))) object)))
    (enqueue (input-queue socket) (cdr delimiter))
    (setf (cdr delimiter) (dequeue (output-queue socket)))
    (send (target peer) object)))

(defmethod die :after ((socket rep-socket) peer)
  (setf (peers socket) (delete peer (peers socket))))

(defmethod send ((socket rep-socket) (message cons))
  (enqueue (output-queue socket) message))

;;; The DEALER Socket Type
;;; https://rfc.zeromq.org/spec/28/#the-dealer-socket-type

(defclass dealer-socket (socket)
  ((%peers :accessor peers
           :initform nil)
   (%next :accessor next
          :initform nil)))

(defmethod socket-type ((socket dealer-socket))
  "DEALER")

(defmethod start-peer :after ((socket dealer-socket) peer)
  (push peer (peers socket)))

(defmethod poll :after ((socket dealer-socket))
  (mapc #'poll (peers socket)))

(defmethod make-socket ((type (eql :dealer)) &key (context *context*))
  (let ((socket (make-instance 'dealer-socket :context context)))
    (enqueue-task socket (lambda ()
                           (poll socket)
                           t))
    socket))

(defmethod send ((socket dealer-socket) (message cons))
  (with-accessors ((peers peers)
                   (next next))
      socket
    (when peers
      (tagbody
       repeat
         (when (queue-empty-p (output-queue (car next)))
           (send (car next) message)
           (setf next (cdr next))
           (return-from send nil))
         (setf next (or (cdr next) peers))
         (go repeat)))))

(defmethod process ((socket dealer-socket) (peer peer) (object cons))
  (enqueue (input-queue socket) object))

;;; The ROUTER Socket Type
;;; https://rfc.zeromq.org/spec/28/#the-router-socket-type

(defclass router-socket (address-socket)
  ())

(defmethod socket-type ((socket router-socket))
  "ROUTER")

(defmethod die :after ((socket router-socket) peer)
  (remove-peer socket peer))

(defmethod start-peer :after ((socket router-socket) peer)
  (add-peer socket peer))

(defmethod poll :after ((socket router-socket))
  (map-peers socket #'poll))

(defmethod make-socket ((type (eql :router)) &key (context *context*))
  (let ((socket (make-instance 'router-socket :context context)))
    (enqueue-task socket (lambda ()
                           (poll socket)
                           t))
    socket))

(defmethod send ((socket router-socket) (message cons))
  (let ((peer (find-peer socket (car message))))
    (if peer
        (send peer (cdr message))
        (error "Unable to find peer for ~s routing-id" (car message)))))

(defmethod process ((socket router-socket) (peer peer) (object cons))
  (enqueue (input-queue socket) (cons (routing-id peer) object)))
