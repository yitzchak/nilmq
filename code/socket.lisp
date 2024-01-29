(in-package #:nilmq)

(defun write-greeting (socket mechanism serverp
                       &aux (target (target socket)))
  (write-sequence +signature+ target)
  (write-byte +version-major+ target)
  (write-byte +version-minor+ target)
  (write-vstring target mechanism 20)
  (write-byte (if serverp #b0 #b1) target)
  (write-sequence +padding+ target)
  nil)

(defun read-greeting (socket)
  (let ((signature (make-array 10 :element-type '(unsigned-byte 8)))
        (padding (make-array 10 :element-type '(unsigned-byte 31)))
        (target (target socket)))
    (read-sequence signature target)
    (multiple-value-prog1
        (values (read-byte target)
                (read-byte target)
                (read-vstring target 20))
      (read-sequence padding target))))

(defun parse-endpoint (endpoint)
  (multiple-value-bind (protocol address)
      (split-sequence endpoint "://")
    (cond ((equalp protocol "tcp")
           (multiple-value-bind (interface port resource)
               (split-sequence address ":" "/")
             (values :tcp
                     interface
                     (if (equalp port "*")
                         0
                         (parse-integer port))
                     resource)))
          (t
           (error "Unknown protocol ~a" protocol)))))

(defclass endpoint ()
  ((%address :reader address
             :initarg :address)
   (%handle :reader handle
            :initarg :handle)))

(defclass server (endpoint)
  ((%connections :accessor connections
                 :initform nil)))

(defclass client (endpoint)
  ((%connection :accessor connection
                :initarg :connection)))

(defclass connection ()
  ((%input-queue :reader input-queue
                 :initform (make-instance 'queue))
   (%output-queue :reader output-queue
                  :initform (make-instance 'queue))
   (%handle :reader handle
            :initform :handle)))

(defmethod send ((connection connection) object)
  (enqueue (output-queue connection) object))

(defclass identity-connection (connection)
  ((%identity :accessor identity
              :initarg :identity
              :initform nil)))

(defclass sub-connection (connection)
  ((%subscriptions :accessor subscriptions
                   :initarg :subscriptions
                   :initform nil)))

(defmethod send :around ((connection connection) (message cons))
  (when (some (lambda (sub)
                (not (mismatch sub (car message) :end1 (length sub))))
              (subscriptions connection))
    (call-next-method)))

(defmethod target ((object connection))
  (usocket:socket-stream (handle object)))

(defclass socket ()
  ((object-factories :reader object-factories
                     :initform (let ((factories (make-hash-table :test #'equalp)))
                                 (setf (gethash "READY" factories)
                                       (lambda (name)
                                         (declare (ignore name))
                                         (make-instance 'ready-command))
                                       (gethash "ERROR" factories)
                                       (lambda (name)
                                         (declare (ignore name))
                                         (make-instance 'error-command)))
                                 factories))
   (endpoints :reader endpoints
              :initform (make-hash-table :test #'equalp))))

(defun default-object-factory (name)
  (if (numberp name)
      (make-instance 'unknown-part)
      (make-instance 'unknown-command :name name)))

(defmethod object-factory ((object socket) name)
  (gethash name (object-factories object) #'default-object-factory))

(defmethod (setf object-factory) (function (object socket) name)
  (setf (gethash name (object-factories object)) function))

(defmethod bind ((socket socket) endpoint)
  (multiple-value-bind (protocol host port resource)
      (parse-endpoint endpoint)
    (declare (ignore protocol))
    (let* ((handle (usocket:socket-listen host port :element-type '(unsigned-byte 8)))
           (endpoint (format nil "tcp://~a:~a~@[/~a~]"
                             host (usocket:get-local-port handle) resource)))
      (setf (gethash endpoint (endpoints socket))
            (make-instance 'server :handle handle :address endpoint))
      endpoint)))

(defmethod connect ((socket socket) endpoint)
  (multiple-value-bind (protocol host port resource)
      (parse-endpoint endpoint)
    (declare (ignore protocol))
    (let* ((handle (usocket:socket-connect host port :element-type '(unsigned-byte 8)))
           (endpoint (format nil "tcp://~a:~a~@[/~a~]"
                             host (usocket:get-local-port handle) resource)))
      (setf (gethash endpoint (endpoints socket))
            (make-instance 'client :handle handle :address endpoint))
      endpoint)))

(defclass nonblocking-socket (socket)
  ((thread :accessor thread)
   (input-queue :reader input-queue
                :initform (make-instance 'queue))))

(defmethod receive ((socket nonblocking-socket))
  (dequeue (input-queue socket)))

(defmethod input-available-p ((socket nonblocking-socket))
  (not (queue-empty-p (input-queue socket))))

(defclass router-socket (nonblocking-socket)
  ((%connections :reader connections
                 :initform (make-hash-table :test #'equalp))))

(defmethod make-socket ((type (eql :router)))
  (make-instance 'router-socket))

(defmethod send ((socket router-socket) (message cons))
  (let ((connection (gethash (car message) (connections socket))))
    (if connection
        (send connection (cdr message))
        (error "Unable to find connection for ~s identity" (car message)))))

(defmethod connect :around ((socket router-socket) endpoint)
  (let ((endpoint (call-next-method)))
    (when endpoint
      (let* ((client (gethash endpoint (endpoints socket)))
             (connection (make-instance 'identity-connection
                                        :handle (handle client))))
        (write-greeting connection "NULL" nil)
        (read-greeting connection)
        (handshake connection) ; This will get an identity
        (setf (gethash (identity connection) (connections socket))
              connection))
      endpoint)))

(defclass pub-socket (nonblocking-socket)
  ((%connections :accessor connections
                 :initform nil)))

(defmethod make-socket ((type (eql :pub)))
  (make-instance 'pub-socket))

(defmethod send ((socket pub-socket) (message cons))
  (loop for connection in (connections socket)
        do (send connection message)))

(defmethod connect :around ((socket pub-socket) endpoint)
  (let ((endpoint (call-next-method)))
    (when endpoint
      (let* ((client (gethash endpoint (endpoints socket)))
             (connection (make-instance 'identity-connection
                                        :handle (handle client))))
        (handshake connection)
        (push connection (connections socket)))
      endpoint)))

(defmethod send (stream command)
  (let ((name (name command)))
    (multiple-value-bind (data-size data)
        (serialize-data command)
      (let ((size (+ 1 (length name) data-size)))
        (cond ((< size 256)
               (write-byte (ash 1 +command-bit+) stream)
               (write-byte size stream))
              (t
               (write-byte (logior (ash 1 +command-bit+)
                                   (ash 1 +long-bit+))
                           stream)
               (write-uint64 size stream)))
        (write-vstring name stream)
        (send-data stream command data)))))

(defmethod send (stream (message cons))
  (loop for (part . rest) on message
        for (size data) = (multiple-value-list (serialize-data part))
        if (< size 256)
          do (write-byte (if rest (ash 1 +more-bit+) 0) stream)
             (write-byte size stream)
        else
          do (write-byte (logior (ash 1 +long-bit+)
                                 (if rest (ash 1 +more-bit+) 0))
                         stream)
             (write-uint64 size stream)
        do (send-data stream part data)))

(defmethod receive (socket)
  (let* ((target (target socket))
         (flags (read-byte target)))
    (if (logbitp +command-bit+ flags)
        (let* ((size (if (logbitp +long-bit+ flags)
                         (read-uint64 stream)
                         (read-byte stream)))
               (name (read-vstring stream))
               (command (funcall (object-factory socket name) name)))
          (receive-data socket command (- size (length name) 1))
          command)
        (loop for index from 0
              for part = (funcall (object-factory socket index) index)
              collect part into message
              do (receive-data socket part
                               (if (logbitp +long-bit+ flags)
                                   (read-uint64 stream)
                                   (read-byte stream))
                               index)
              unless (logbitp +more-bit+ flags)
                return message
              do (setf flags (read-byte target))))))

(defmethod handshake ((socket connection))
  (send socket (make-instance 'ready-command))
  (let ((response (receive socket)))
    (check-type response ready-command)
    (let ((identity (identity response)))
      (when identity
        (setf (identity socket) identity)))))
