(in-package #:nilmq)

(defun split-sequence (seq &rest delimiters)
  (loop with start = 0
        for delimiter in delimiters
        for pos = (search delimiter seq :start2 start)
        finally (return (values-list (if (< start (length seq))
                                         (nconc result (list (subseq seq start)))
                                         result)))
        collect (subseq seq start pos) into result
        if pos
          do (setf start (+ pos (length delimiter)))
        else
          do (setf start (length seq))
             (loop-finish)))

(defun write-uint64 (stream value)
  (write-byte (ldb (byte 8 56) value) stream)
  (write-byte (ldb (byte 8 48) value) stream)
  (write-byte (ldb (byte 8 40) value) stream)
  (write-byte (ldb (byte 8 32) value) stream)
  (write-byte (ldb (byte 8 24) value) stream)
  (write-byte (ldb (byte 8 16) value) stream)
  (write-byte (ldb (byte 8 8) value) stream)
  (write-byte (ldb (byte 8 0) value) stream))

(defun read-uint64 (stream)
  (logior (ash (read-byte stream) 56)
          (ash (read-byte stream) 48)
          (ash (read-byte stream) 40)
          (ash (read-byte stream) 32)
          (ash (read-byte stream) 24)
          (ash (read-byte stream) 16)
          (ash (read-byte stream) 8)
          (read-byte stream)))

(defun write-uint32 (stream value)
  (write-byte (ldb (byte 8 24) value) stream)
  (write-byte (ldb (byte 8 16) value) stream)
  (write-byte (ldb (byte 8 8) value) stream)
  (write-byte (ldb (byte 8 0) value) stream))

(defun read-uint32 (stream)
  (logior (ash (read-byte stream) 24)
          (ash (read-byte stream) 16)
          (ash (read-byte stream) 8)
          (read-byte stream)))

(defun write-vstring (stream value &optional (size :uint8))
  (if (numberp size)
      (loop for pos from 0 below size
            do (write-byte (if (< pos (length value))
                               (char-code (char value pos))
                               0)
                           stream))
      (loop for ch across value
            initially (ecase size
                        (:uint8 (write-byte (length value) stream))
                        (:uint32 (write-uint32 (length value) stream)))
            do (write-byte (char-code ch) stream))))

(defun read-vstring (stream &optional (size :uint8))
  (if (numberp size)
      (loop with result = (make-array size
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
      (loop with size = (ecase size
                          (:uint8 (read-byte stream))
                          (:uint32 (read-uint32 stream)))
            with result = (make-array size :element-type 'standard-char)
            for pos from 0 below size
            finally (return result)
            do (setf (schar result pos) (code-char (read-byte stream))))))

(defclass binary-output-stream
    (ngray:fundamental-binary-output-stream)
  ((buffer :accessor buffer
           :initform (make-array 64
                                 :adjustable t
                                 :fill-pointer 0
                                 :element-type '(unsigned-byte 8)))))

(defmethod ngray:stream-element-type ((stream binary-output-stream))
  '(unsigned-byte 8))

(defmethod ngray:stream-write-byte ((stream binary-output-stream) value)
  (vector-push-extend value (buffer stream)))

(defun get-output-stream-sequence (stream)
  (prog1
      (buffer stream)
    (setf (buffer stream)
          (make-array 64
                      :adjustable t
                      :fill-pointer 0
                      :element-type '(unsigned-byte 8)))))
