(in-package #:nilmq)

(defun write-greeting (stream mechanism serverp)
  (write-sequence +signature+ stream)
  (write-byte +version-major+ stream)
  (write-byte +version-minor+ stream)
  (write-vstring stream mechanism 20)
  (write-byte (if serverp #b0 #b1) stream)
  (write-sequence +padding+ stream)
  (finish-output stream)
  nil)

(defvar a nil)

(defun read-greeting (stream)
  (let ((signature (make-array 10 :element-type '(unsigned-byte 8)))
        (padding (make-array 31 :element-type '(unsigned-byte 8))))
    (read-sequence signature stream)
    (multiple-value-prog1
        (values (read-byte stream)
                (read-byte stream)
                (read-vstring stream 20)
                (read-byte stream))
      (read-sequence padding stream))))

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
  ((%parent :accessor parent
            :initarg :parent)
   (%address :reader address
             :initarg :address)
   (%handle :reader handle
            :initarg :handle)))

(defclass server (endpoint)
  ((%connections :accessor connections
                 :initform nil)))

(defmethod poll ((server server))
  (when (usocket:socket-state (handle server))
    (push (usocket:socket-state (handle server)) a)
    (let ((connection (make-instance 'connection
                                     :parent (parent server)
                                     :handle (usocket:socket-accept (handle server)))))
      (push connection (connections server))
      connection)))

(defclass client (endpoint)
  ((%connection :accessor connection
                :initarg :connection)))

(defclass connection ()
  ((%parent :accessor parent
            :initarg :parent)
   (%output-queue :reader output-queue
                  :initform (make-instance 'queue))
   (%handle :reader handle
            :initarg :handle)
   (%routing-id :accessor routing-id
              :initarg :routing-id
              :initform nil)
   (%subscriptions :accessor subscriptions
                   :initarg :subscriptions
                   :initform nil)))

(defmethod input-available-p ((connection connection))
  (not (queue-empty-p (input-queue connection))))

(defmethod object-factory ((object connection) name)
  (object-factory (parent object) name))

(defmethod send ((connection connection) object)
  (enqueue (output-queue connection) object))

(defmethod process (socket (connection connection) (object subscribe-command))
  (declare (ignore socket))
  (push (data object) (subscriptions connection)))

(defmethod process (socket (connection connection) (object cancel-command))
  (declare (ignore socket))
  (setf (subscriptions connection)
        (delete (data object) (subscriptions connection)
                :test #'equalp)))

(defmethod poll ((connection connection))
  (when (usocket:socket-state (handle connection))
    (process (parent connection) connection (receive connection)))
  (unless (queue-empty-p (output-queue connection))
    (send (target connection)
          (dequeue (output-queue connection))))
  nil)

(defmethod target ((object connection))
  (usocket:socket-stream (handle object)))

(defclass socket ()
  ((object-factories :reader object-factories
                     :initform (let ((factories (make-hash-table :test #'equalp)))
                                 (setf (gethash "READY" factories)
                                       (lambda (name size)
                                         (declare (ignore name size))
                                         (make-instance 'ready-command))
                                       (gethash "ERROR" factories)
                                       (lambda (name size)
                                         (declare (ignore name size))
                                         (make-instance 'error-command))
                                       (gethash "SUBSCRIBE" factories)
                                       (lambda (name size)
                                         (declare (ignore name size))
                                         (make-instance 'subscribe-command))
                                       (gethash "CANCEL" factories)
                                       (lambda (name size)
                                         (declare (ignore name size))
                                         (make-instance 'cancel-command)))
                                 factories))
   (endpoints :reader endpoints
              :initform (make-hash-table :test #'equalp))
   (wait-list :accessor wait-list
              :initform nil)
   (routing-id :accessor routing-id
               :initarg :routing-id
               :initform nil)))

(defgeneric handles (object)
  (:method (object)
    (declare (ignore object))
    nil))

(defmethod handles ((client client))
  (list (handle client)))

(defmethod handles ((server server))
  (list* (handle server)
         (mapcar #'handle (connections server))))

(defun update-wait-list (socket)
  (setf (wait-list socket)
        (usocket:make-wait-list
         (loop for endpoint being the hash-values of (endpoints socket)
               nconc (handles endpoint)))))

(defun default-object-factory (name size)
  (if (numberp name)
      (make-array size :element-type '(unsigned-byte 8))
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
            (make-instance 'server :handle handle :address endpoint :parent socket))
      (update-wait-list socket)
      endpoint)))

(defmethod connect ((socket socket) endpoint)
  (multiple-value-bind (protocol host port resource)
      (parse-endpoint endpoint)
    (declare (ignore protocol))
    (let* ((handle (usocket:socket-connect host port :element-type '(unsigned-byte 8)))
           (endpoint (format nil "tcp://~a:~a~@[/~a~]"
                             host (usocket:get-local-port handle) resource)))
      (setf (gethash endpoint (endpoints socket))
            (make-instance 'client :handle handle :address endpoint :parent socket)
            (connection endpoint) (make-instance 'connection :handle handle :parent socket))
      (start-connection socket (connection endpoint))
      endpoint)))

(defmethod poll ((socket socket))
  (usocket:wait-for-input (wait-list socket)
                          :ready-only t
                          :timeout .05)
  (loop for endpoint being the hash-value of (endpoints socket)
        for connection = (poll endpoint)
        when connection
          do (start-connection socket connection)))

(defclass nonblocking-socket (socket)
  ((thread :accessor thread)
   (input-queue :reader input-queue
                :initarg :input-queue
                :initform (make-instance 'queue))))

(defmethod receive ((socket nonblocking-socket))
  (dequeue (input-queue socket)))

(defmethod input-available-p ((socket nonblocking-socket))
  (not (queue-empty-p (input-queue socket))))

(defmethod start-connection (socket connection)
  (write-greeting (target connection) "NULL" nil)
  (read-greeting (target connection))
  (handshake connection)
  (update-wait-list socket))

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
        (write-vstring stream name)
        (send-data stream command data))))
  (finish-output stream))

(defmethod send (stream (message cons))
  (loop for (part . rest) on message
        for (size data) = (multiple-value-list (serialize-data part))
        finally (finish-output stream)
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
  (let* ((stream (target socket))
         (flags (read-byte stream)))
    (push flags a)
    (if (logbitp +command-bit+ flags)
        (let* ((size (if (logbitp +long-bit+ flags)
                         (read-uint64 stream)
                         (read-byte stream)))
               (name (read-vstring stream))
               (data-size (- size (length name) 1))
               (command (funcall (object-factory socket name) name data-size)))
          (receive-data socket command data-size)
          command)
        (loop for index from 0
              for size = (if (logbitp +long-bit+ flags)
                                   (read-uint64 stream)
                                   (read-byte stream))
              for part = (funcall (object-factory socket index) index size)
              collect part into message
              do (receive-data socket part size)
              unless (logbitp +more-bit+ flags)
                return message
              do (setf flags (read-byte stream))))))

(defmethod handshake ((socket connection))
  (send socket (make-instance 'ready-command
                              :routing-id (routing-id (parent socket))
                              :socket-type (socket-type (parent socket))))
  (let ((response (receive socket)))
    (check-type response ready-command)
    (push response a)
    (let ((routing-id (routing-id response)))
      (when routing-id
        (setf (routing-id socket) routing-id)))))
