(in-package #:nilmq)

(defgeneric context (instance))

(defgeneric close (instance))

(defgeneric enqueue-task (instance func))

(defgeneric add-peer (self peer))

(defgeneric remove-peer (self peer))

(defgeneric find-peer (self &optional id))

(defgeneric routing-id (object))

(defgeneric make-socket (type &key context))

(defgeneric subscribe (socket topic))

(defgeneric unsubscribe (socket topic))

(defgeneric bind (socket endpoint))

(defgeneric connect (socket endpoint))

(defgeneric input-available-p (socket))

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

(defgeneric process (socket peer object)
  (:method (socket peer object)
    (declare (ignore socket peer object))))

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
