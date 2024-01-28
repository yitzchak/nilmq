(in-package #:nilmq)

(defgeneric shutdown (socket))

(defgeneric resource (socket))

(defgeneric target (socket))

(defgeneric name (object)
  (:method (object)
    (declare (ignore object))
    nil))

(defgeneric serialize-data (socket object &optional index))

(defgeneric send-data (socket object data &optional index))

(defgeneric send (socket object))

(defgeneric receive (socket))

(defgeneric object-factory (socket name))

(defgeneric (setf object-factory) (function socket name))

(defgeneric receive-data (socket object size &optional index))

(defgeneric handshake (socket))

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
