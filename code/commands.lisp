(in-package #:nilmq)

(defclass unknown-part ()
  ((data :accessor data)))

(defclass unknown-command ()
  ((name :reader name
         :initarg :name)
   (data :accessor data)))

(defclass metadata-mixin ()
  ((metadata :reader metadata
             :initform (make-hash-table :test #'equalp))))

(defmethod serialize-data ((object metadata-mixin))
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

(defmethod send-data (stream (object metadata-mixin) data)
  (write-sequence data stream))

(defclass ready-command (metadata-mixin)
  ())

(defclass error-command ()
  ((reason :accessor reason
           :initform "")))

(defmethod receive-data (socket (object unknown-command) size)
  (read-sequence (setf (data object)
                       (make-array size :element-type '(unsigned-byte 8)))
                 (target stream))
  object)

(defmethod serialize-data ((object error-command))
  (1+ (length (reason object))))

(defmethod send-data (stream (command error-command) data)
  (declare (ignore data))
  (write-vstring stream (reason object)))
