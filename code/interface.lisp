(in-package #:nilmq)

(defgeneric poll (object)
  (:method (object)
    (declare (ignore object))
    nil))

(defgeneric routing-id (object))

(defgeneric make-socket (type))

(defgeneric start-connection (socket connection))

(defgeneric bind (socket endpoint))

(defgeneric connect (socket endpoint))

(defgeneric input-available-p (socket))

(defgeneric shutdown (socket))

(defgeneric resource (socket))

(defgeneric target (socket))

(defgeneric socket-type (socket))

(defgeneric name (object)
  (:method (object)
    (declare (ignore object))
    nil))

(defgeneric serialize-data (object))

(defgeneric send-data (socket object data))

(defgeneric send (socket object))

(defgeneric receive (socket))

(defgeneric object-factory (socket name))

(defgeneric (setf object-factory) (function socket name))

(defgeneric receive-data (socket object size))

(defgeneric handshake (socket))

(defgeneric process (socket connection object)
  (:method (socket connection object)
    (declare (ignore socket connection object))))

(defconstant +more-bit+ 0)

(defconstant +long-bit+ 1)

(defconstant +command-bit+ 2)

(defconstant +reserved-bits+
  #b11111000)

(defconstant +version-major+ 3)

(defconstant +version-minor+ 1)

(defparameter +signature+
  #(#xff #x00 #x00 #x00 #x00 #x00 #x00 #x00 #x01 #x7f))

(defparameter +padding+
  (make-array 31 :element-type '(unsigned-byte 8) :initial-element 0))
