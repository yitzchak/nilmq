(in-package #:nilmq)

(defclass router-socket (nonblocking-socket)
  ((%connections :reader connections
                 :initform (make-hash-table :test #'equalp))))

(defmethod socket-type ((socket router-socket))
  "ROUTER")

(defmethod start-connection :after ((socket router-socket) connection)
  (setf (gethash (routing-id connection) (connections socket))
        connection))

(defmethod poll :after ((socket router-socket))
  (loop for connection being the hash-values of (connections socket)
        do (poll connection)
        do (loop with routing-id = (routing-id connection)
                 while (input-available-p connection)
                 do (enqueue (input-queue socket)
                             (cons routing-id (dequeue (input-queue connection)))))))

(defmethod make-socket ((type (eql :router)))
  (let ((socket (make-instance 'router-socket)))
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

(defmethod make-socket ((type (eql :dealer)))
  (let ((socket (make-instance 'dealer-socket)))
    (setf (thread socket)
          (bordeaux-threads:make-thread
           (lambda ()
             (loop (poll socket)))))
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
