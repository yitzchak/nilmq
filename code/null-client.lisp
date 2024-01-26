(in-package #:nilmq)

(defclass metadata-mixin ()
  ((metadata :reader metadata
             :initform (make-hash-table :test #'equalp))))

(defmethod serialize-command-data (client (command metadata-mixin))
  (loop for k being the hash-keys in (metadata command)
          using (hash-value v)
        sum (+ 1 (length k) (length v))))

(defmethod write-command-data (client stream (command metadata-mixin) data)
  (declare (ignore data))
  (loop for k being the hash-keys in (metadata command)
          using (hash-value v)
        do (write-vstring stream k)
           (write-sequence v stream)))

(defclass ready-command (metadata-mixin)
  ())

(defclass error-command ()
  ((reason :accessor reason
           :initform "")))

(defclass null-client ()
  ((command-classes :reader command-classes
                    :initform (make-hash-table :test #'equalp))))

(defmethod initialize-instance :after ((instance null-client) &rest initargs &key)
  (declare (ignore initargs))
  (with-accessors ((command-classes command-classes))
      instance
    (setf (gethash "READY" command-classes) 'ready-command
          (gethash "ERROR" command-classes) 'error-command)))

(defclass unknown-command ()
  ((name :reader command-name
         :initarg :name)
   (data :accessor command-data)))

(defmethod make-command ((client null-client) name)
  (make-instance (gethash name (command-classes client) 'unknown-command)
                 :name name))

(defmethod read-command-data (client stream (command unknown-command) size)
  (read-sequence (setf (command-data command)
                       (make-array size :element-type '(unsigned-byte 8)))
                 stream)
  command)

(defmethod serialize-command-data (client (command error-command))
  (declare (ignore client))
  (1+ (length (reason command))))

(defmethod write-command-data (client stream (command error-command) data)
  (declare (ignore client data))
  (write-vstring stream (reason command)))

