(in-package #:nilmq)

(defmethod send (socket object)
  (let ((name (name object))
        (target (target socket)))
    (if name
        (multiple-value-bind (data-size data)
            (serialize-data socket object)
          (let ((size (+ 1 (length name) data-size)))
            (cond ((< size 256)
                   (write-byte (ash 1 +command-bit+) target)
                   (write-byte size target))
                  (t
                   (write-byte (logior (ash 1 +command-bit+)
                                       (ash 1 +long-bit+))
                               target)
                   (write-uint64 size target)))
            (write-vstring name target)
            (send-data socket object data)))
        (prog ((index 0))
         repeat
           (multiple-value-bind (size data morep)
               (serialize-data socket object index)
             (cond ((< size 256)
                    (write-byte (if morep (ash 1 +more-bit+) 0) target)
                    (write-byte size target))
                   (t
                    (write-byte (logior (ash 1 +long-bit+)
                                        (if morep (ash 1 +more-bit+) 0))
                                target)
                    (write-uint64 size target)))
             (send-data socket object data index)
             (when morep
               (incf index)
               (go repeat))))))
  nil)

(defmethod receive (socket)
  (let* ((target (target socket))
         (flags (read-byte target)))
    (if (logbitp +command-bit+ flags)
        (let* ((size (if (logbitp +long-bit+ flags)
                         (read-uint64 stream)
                         (read-byte stream)))
               (name (read-vstring stream))
               (object (funcall (object-factory socket name) name)))
          (receive-data socket object (- size (length name) 1))
          object)
        (prog ((object (funcall (object-factory socket nil) nil))
               (index 0))
         repeat
           (receive-data socket object
                         (if (logbitp +long-bit+ flags)
                             (read-uint64 stream)
                             (read-byte stream))
                         index)
           (unless (logbitp +more-bit+ flags)
             (return object))
           (setf flags (read-byte stream))
           (incf index)
           (go repeat)))))

(defclass metadata-mixin ()
  ((metadata :reader metadata
             :initform (make-hash-table :test #'equalp))))

(defmethod serialize-data (socket (object metadata-mixin) &optional index)
  (declare (ignore socket index))
  (let ((stream (make-instance 'binary-output-stream)))
    (loop for def = (closer-mop:class-slots (class-of object))
          for slot-name = (closer-mop:slot-definition-name def)
          when (slot-boundp object slot-name)
            do (write-vstring (symbol-name slot-name) stream)
               (let ((value (slot-value value slot-name)))
                 (typecase value
                   (string
                    (write-vstring value stream :uint32))
                   (otherwise
                    (write-uint32 (length value) stream)
                    (write-sequence value stream)))))
    (loop for k being the hash-keys in (metadata object)
            using (hash-value value)
          do (write-vstring stream k)
             (typecase value
               (string
                (write-vstring value stream :uint32))
               (otherwise
                (write-uint32 (length value) stream)
                (write-sequence value stream))))
    (values (length (buffer stream))
            (buffer stream))))

(defmethod send-data (socket (object metadata-mixin) data &optional index)
  (declare (ignore index))
  (write-sequence data (target socket)))

(defclass unknown-command ()
  ((name :reader name
         :initarg :name)
   (data :accessor data)))

(defclass unknown-message ()
  ((data :accessor data)))

(defclass ready-command (metadata-mixin)
  ())

(defclass error-command ()
  ((reason :accessor reason
           :initform "")))

(defclass null-socket ()
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
   (protocol :accessor socket-protocol)
   (%handle :reader handle
            :accessor socket-handle)
   (%target :reader target
            :accessor socket-target)
   (%resource :reader resource
            :accessor socket-resource)))

(defun default-object-factory (name)
  (if name
      (make-instance 'unknown-command :name name)
      (make-instance 'unknown-message)))

(defmethod object-factory ((socket null-socket) name)
  (gethash name (object-factories socket) #'default-object-factory))

(defmethod (setf object-factory) (function (socket null-socket) name)
  (setf (gethash name (object-factories)) function))

(defmethod handshake ((socket null-socket))
  (send socket (make-instance 'ready-command))
  (let ((response (read socket)))
    (check-type response ready-command)))

(defmethod receive-data (socket (object unknown-command) size &optional index)
  (declare (ignore index))
  (read-sequence (setf (data object)
                       (make-array size :element-type '(unsigned-byte 8)))
                 (target stream))
  object)

(defmethod serialize-data (socket (object error-command) &optional index)
  (declare (ignore socket index))
  (1+ (length (reason object))))

(defmethod send-data (socket (command error-command) data &optional index)
  (declare (ignore client data index))
  (write-vstring (target socket) (reason object)))

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

(defun connect (endpoint &key mechanism)
  (let ((socket (ecase mechanism
                  ((nil)
                   (make-instance 'null-socket))))
        protocol ip port)
    (with-accessors ((handle socket-handle)
                     (resource socket-resource)
                     (target socket-target))
        socket
      (multiple-value-setq (protocol ip port resource)
        (split-sequence endpoint "://" ":" "/"))
      (assert (equalp protocol "tcp"))
      (setf port (if (string= port "*") 0 (parse-integer port))
            handle (usocket:socket-connect ip port :element-type '(unsigned-byte 8))
            target (usocket:socket-stream handle)))
    (write-greeting socket "NULL" nil)
    (read-greeting socket)
    (handshake socket)
    socket))
