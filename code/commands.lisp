(in-package #:nilmq)

(defclass octet-mixin ()
  ((data :accessor data)))

(defmethod receive-data (socket (object octet-mixin) size)
  (read-sequence (setf (data object)
                       (make-array size :element-type '(unsigned-byte 8)))
                 (target socket))
  object)

(defclass unknown-command (octet-mixin)
  ((name :reader name
         :initarg :name)))

(defclass metadata-mixin ()
  ((metadata :reader metadata
             :initform (make-hash-table :test #'equalp))))

(defmethod serialize-data ((object metadata-mixin))
  (let ((stream (make-instance 'binary-output-stream)))
    (loop for def in (closer-mop:class-slots (class-of object))
          for slot-name = (closer-mop:slot-definition-name def)
          when (and (slot-boundp object slot-name)
                    (not (eq slot-name 'metadata)))
            do (write-vstring stream (symbol-name slot-name))
               (let ((value (slot-value object slot-name)))
                 (typecase value
                   (string
                    (write-vstring stream value :uint32))
                   (otherwise
                    (write-uint32 stream (length value))
                    (write-sequence value stream)))))
    (loop for k being the hash-keys of (metadata object)
            using (hash-value value)
          do (write-vstring stream k)
             (typecase value
               (string
                (write-vstring stream value :uint32))
               (otherwise
                (write-uint32 stream (length value))
                (write-sequence value stream))))
    (values (length (buffer stream))
            (buffer stream))))

(defmethod send-data (stream (object metadata-mixin) data)
  (write-sequence data stream))

(defmethod receive-data (socket (object metadata-mixin) size)
  (prog (key len value def
         (stream (target socket))
         (slots (closer-mop:class-slots (class-of object))))
   repeat
     (unless (zerop size)
       (setf key (read-vstring stream)
             def (find key slots :key (lambda (def)
                                        (symbol-name (closer-mop:slot-definition-name def)))
                                 :test #'equalp))
       (cond ((null def)
              (setf len (read-uint32 stream)
                    value (make-array len :element-type '(unsigned-byte 8)))
              (decf size (+ 5 (length key) len))
              (read-sequence value stream)
              (setf (gethash key (metadata object)) value))
             ((subtypep (closer-mop:slot-definition-type def) 'string)
              (setf value (read-vstring stream :uint32)
                    (slot-value object (closer-mop:slot-definition-name def)) value)
              (decf size (+ 5 (length key) (length value))))
             (t
              (setf len (read-uint32 stream)
                    value (make-array len :element-type '(unsigned-byte 8)))
              (decf size (+ 5 (length key) len))
              (read-sequence value stream)
              (setf (slot-value object (closer-mop:slot-definition-name def)) value)))
       (go repeat))))

(defclass ready-command (metadata-mixin)
  ((identity :accessor routing-id
             :initarg :routing-id
             :initform nil
             :type (vector (unsigned-byte 8) *))
   (socket-type :accessor socket-type
                :initarg :socket-type
                :type string)
   (resource :accessor resource
             :initarg :socket-type
             :type string)))

(defmethod name ((command ready-command))
  "READY")

(defclass error-command ()
  ((reason :accessor reason
           :initform "")))

(defmethod name ((command error-command))
  "ERROR")

(defmethod serialize-data ((object error-command))
  (1+ (length (reason object))))

(defmethod send-data (stream (command error-command) data)
  (declare (ignore data))
  (write-vstring stream (reason object)))

(defclass subscribe-command (octet-mixin)
  ())

(defmethod name ((command subscribe-command))
  "SUBSCRIBE")

(defclass cancel-command (octet-mixin)
  ())

(defmethod name ((command subscribe-command))
  "CANCEL")

(defclass ping-command ()
  ((ttl :accessor ttl
        :initarg :ttl)
   (context :accessor context
            :initarg :context)))

(defmethod serialize-data ((object ping-command))
  (+ 2 (length (context object))))

(defmethod send-data (stream (command ping-command) data)
  (declare (ignore data))
  (write-uint16 stream (ttl object))
  (write-sequence (context stream) stream))

(defmethod receive-data (socket (object ping-command) size)
  (setf (ttl object) (read-uint16 stream)
        (context object) (make-array (- size 2) :element-type '(unsigned-byte 8)))
  (read-sequence (context object) stream))

(defmethod process (socket peer (object ping-command))
  (declare (ignore socket))
  (send peer (make-instance 'pong-command :context (context ping-command))))

(defclass pong-command ()
  ((context :accessor context
            :initarg :context)))

(defmethod serialize-data ((object pong-command))
  (length (context object)))

(defmethod send-data (stream (command pong-command) data)
  (declare (ignore data))
  (write-sequence (context stream) stream))

(defmethod receive-data (socket (object pong-command) size)
  (setf (context object) (make-array size :element-type '(unsigned-byte 8)))
  (read-sequence (context object) stream))

(defmethod receive-data (socket (object vector) size)
  (declare (ignore size))
  (read-sequence object (target socket))
  object)

(defmethod serialize-data ((object string))
  (length object))

(defmethod serialize-data ((object vector))
  (length object))

(defmethod send-data (stream (object string) data)
  (declare (ignore data))
  (write-vstring stream object nil))

(defmethod send-data (stream (object vector) data)
  (declare (ignore data))
  (write-sequence object stream))
