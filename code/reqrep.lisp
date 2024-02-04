(in-package #:nilmq)

(defclass rep-socket (nonblocking-socket)
  ((%output-queue :accessor output-queue
                  :initform (make-instance 'queue))
   (%connections :accessor connections
                 :initform nil)))

(defmethod socket-type ((socket rep-socket))
  "REP")

(defmethod make-socket ((type (eql :rep)) &key (context *context*))
  (let ((socket (make-instance 'rep-socket :context context)))
    (enqueue-task socket (lambda ()
                           (poll socket)
                           t))
    socket))

(defmethod start-connection :after ((socket rep-socket) connection)
  (push connection (connections socket)))

(defmethod poll :after ((socket rep-socket))
  (mapc #'poll (connections socket)))

(defmethod process ((socket rep-socket) (connection connection) (object cons))
  (let ((delimiter (member-if (lambda (x) (zerop (length x))) object)))
    (enqueue (input-queue socket) (cdr delimiter))
    (setf (cdr delimiter) (dequeue (output-queue socket)))
    (send (target connection) object)))

(defmethod die :after ((socket rep-socket) connection)
  (setf (connections socket) (delete connection (connections socket))))

(defmethod send ((socket rep-socket) (message cons))
  (enqueue (output-queue socket) message))

(defclass router-socket (nonblocking-socket)
  ((%connections :reader connections
                 :initform (make-hash-table :test #'equalp))))

(defmethod socket-type ((socket router-socket))
  "ROUTER")

(defmethod die :after ((socket router-socket) connection)
  (remhash (routing-id connection) (connections socket)))

(defmethod start-connection :after ((socket router-socket) connection)
  (setf (gethash (routing-id connection) (connections socket))
        connection))

(defmethod poll :after ((socket router-socket))
  (loop for connection being the hash-values of (connections socket)
        do (poll connection)))

(defmethod make-socket ((type (eql :router)) &key (context *context*))
  (let ((socket (make-instance 'router-socket :context context)))
    (setf (thread socket)
          (bordeaux-threads:make-thread
           (lambda ()
             (loop (poll socket)))))
    socket))

(defmethod send ((socket router-socket) (message cons))
  (let ((connection (gethash (car message) (connections socket))))
    (if connection
        (send connection (cdr message))
        (error "Unable to find connection for ~s routing-id" (car message)))))

(defmethod process ((socket router-socket) (connection connection) (object cons))
  (enqueue (input-queue socket) (cons (routing-id connection) object)))

(defclass dealer-socket (nonblocking-socket)
  ((%connections :accessor connections
                 :initform nil)
   (%next :accessor next
          :initform nil)))

(defmethod socket-type ((socket dealer-socket))
  "DEALER")

(defmethod start-connection :after ((socket dealer-socket) connection)
  (push connection (connections socket)))

(defmethod poll :after ((socket dealer-socket))
  (mapc #'poll (connections socket)))

(defmethod make-socket ((type (eql :dealer)) &key (context *context*))
  (let ((socket (make-instance 'dealer-socket :context context)))
    (enqueue-task socket (lambda ()
                           (poll socket)
                           t))
    socket))

(defmethod send ((socket dealer-socket) (message cons))
  (with-accessors ((connections connections)
                   (next next))
      socket
    (when connections
      (tagbody
       repeat
         (when (queue-empty-p (output-queue (car next)))
           (send (car next) message)
           (setf next (cdr next))
           (return-from send nil))
         (setf next (or (cdr next) connections))
         (go repeat)))))

(defmethod process ((socket dealer-socket) (connection connection) (object cons))
  (enqueue (input-queue socket) object))
