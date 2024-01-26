(in-package #:nilmq)

(defconstant +more-bit+ 0)

(defconstant +long-bit+ 1)

(defconstant +command-bit+ 2)

(defconstant +reserved-bits+
  #b11111000)

(defconstant +version-major+ 3)

(defconstant +version-minor+ 1)

(defparameter +signature+
  #(#xff #x00 #x00 #x00 #x00 #x00 #x00 #x00 #x00 #x7f))

(defparameter +padding+
  (make-array 31 :element-type '(unsigned-byte 8) :initial-element 0))

(defun write-u64 (stream value)
  (write-byte (ldb (byte 8 56) value) stream)
  (write-byte (ldb (byte 8 48) value) stream)
  (write-byte (ldb (byte 8 40) value) stream)
  (write-byte (ldb (byte 8 32) value) stream)
  (write-byte (ldb (byte 8 24) value) stream)
  (write-byte (ldb (byte 8 16) value) stream)
  (write-byte (ldb (byte 8 8) value) stream)
  (write-byte (ldb (byte 8 0) value) stream))

(defun read-u64 (stream)
  (logior (ash (read-byte stream) 56)
          (ash (read-byte stream) 48)
          (ash (read-byte stream) 40)
          (ash (read-byte stream) 32)
          (ash (read-byte stream) 24)
          (ash (read-byte stream) 16)
          (ash (read-byte stream) 8)
          (read-byte stream)))

(defun write-vstring (stream value &optional size)
  (if size
      (loop for pos from 0 below size
            do (write-byte (if (< pos (length value))
                               (char-code (char value pos))
                               0)
                           stream))
      (loop for ch across value
              initially (write-byte (length value) stream)
            do (write-byte (char-code value) stream))))

(defun read-vstring (stream &optional size)
  (if size
      (loop with result = (make-string size
                                       :element-type 'standard-char
                                       :fill-pointer 0)
            with fill = t
            for pos from 0 below size
            for byte = (read-byte stream)
            finally (return result)
            if (zerop byte)
              do (setf fill nil)
            else if fill
                   do (vector-push-extend (code-char byte) result))
      (loop with size = (read-byte stream)
            with result = (make-string size :element-type 'standard-char)
            for pos from 0 below size
            finally (return result)
            do (setf (schar result pos) (code-char (read-byte stream))))))

(defgeneric serialize-command-data (client command))

(defgeneric command-name (command))

(defgeneric write-command-data (client stream command data))

(defun write-command (client stream command)
  (multiple-value-bind (data-size data)
      (serialize-command-data client command)
    (let* ((name (command-name command))
           (size (+ 1 (length name) data-size)))
      (cond ((< size 256)
             (write-byte (ash 1 +command-bit+) stream)
             (write-byte size stream))
            (t
             (write-byte (logior (ash 1 +command-bit+)
                                 (ash 1 +long-bit+))
                         stream)
             (write-u64 size stream)))
      (write-vstring name stream)
      (write-command-data client stream command data)))
  nil)

(defgeneric make-command (client name))

(defgeneric read-command-data (client stream command size))

(defun read-command (client stream longp)
  (let* ((size (if longp
                   (read-u64 stream)
                   (read-byte stream)))
         (name (read-vstring stream)))
    (read-command-data client
                       (make-command client name)
                       (- size (length name) 1))))

(defgeneric serialize-message-part (client message index))

(defgeneric write-message-part (client stream message index data))

(defun write-message (client stream message)
  (prog ((index 0))
   repeat
     (multiple-value-bind (size data morep)
         (serialize-message-part client message index)
       (cond ((< size 256)
              (write-byte (if morep (ash 1 +more-bit+) 0) stream)
              (write-byte size))
             (t
              (write-byte (logior (ash 1 +long-bit+)
                                  (if morep (ash 1 +more-bit+) 0))
                          stream)
              (write-u64 size)))
       (write-message-part client stream message index data)
       (when morep
         (incf index)
         (go repeat))))
  nil)

(defgeneric make-message (client))

(defgeneric read-message-part (client stream message index size morep))

(defun read-message (client stream longp morep)
  (prog ((message (make-message client))
         (index 0)
         size flags)
   repeat
     (setf size (if longp
                    (read-u64 stream)
                    (read-byte stream)))
     (read-message-part client stream message index size morep)
     (unless morep
       (return message)
       (setf flags (read-byte stream))
       (setf longp (logbitp +long-bit+ flags)
             morep (logbitp +more-bit+ flags))
       (go repeat))))

(defun read-traffic (client stream)
  (let ((flags (read-byte stream)))
    (if (logbitp +command-bit+ flags)
        (read-command client stream
                      (logbitp +long-bit+ flags))
        (read-message client stream
                      (logbitp +long-bit+ flags)
                      (logbitp +more-bit+ flags)))))

(defun write-greeting (client stream mechanism serverp)
  (write-sequence +signature+ stream)
  (write-byte +version-major+ stream)
  (write-byte +version-minor+ stream)
  (write-vstring stream mechanism 20)
  (write-byte (if serverp #b0 #b1) stream)
  (write-sequence +padding+ stream)
  nil)

(defun read-greeting (client stream)
  (let ((signature (make-array 10 :element-type '(unsigned-byte 8)))
        (padding (make-array 10 :element-type '(unsigned-byte 31))))
    (read-sequence signature stream)
    (multiple-value-prog1
        (values (read-byte stream)
                (read-byte stream)
                (read-vstring stream 20))
      (read-sequence padding stream))))

(defgeneric handshake (client stream))
