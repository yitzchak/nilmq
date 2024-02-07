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
  ((%peers :accessor peers
           :initform nil)))

(defclass client (endpoint)
  ((%peer :accessor peer
          :initarg :peer)))

(defclass peer ()
  ((%parent :accessor parent
            :initarg :parent)
   (%handle :reader handle
            :initarg :handle)
   (%routing-id :accessor routing-id
                :initarg :routing-id
                :initform nil)
   (%subscriptions :accessor subscriptions
                   :initarg :subscriptions
                   :initform nil)))

(defmethod context ((object peer))
  (context (parent object)))

(defmethod send ((peer peer) object)
  (enqueue-task (parent peer)
                (lambda ()
                  (send (target peer) object)
                  nil)))

(defmethod process (socket (peer peer) (object subscribe-command))
  (declare (ignore socket))
  (push (data object) (subscriptions peer)))

(defmethod process (socket (peer peer) (object cancel-command))
  (declare (ignore socket))
  (setf (subscriptions peer)
        (delete (data object) (subscriptions peer)
                :test #'equalp)))

(defmethod do-read (peer)
  (handler-case
      (process (parent peer) peer (receive peer))
    (error (condition)
      (declare (ignore condition))
      (remove-peer (parent peer) peer)))
  nil)

(defmethod target ((object peer))
  (usocket:socket-stream (handle object)))

(defclass socket (context-mixin)
  ((endpoints :reader endpoints
              :initform (make-hash-table :test #'equalp))
   (routing-id :accessor routing-id
               :initarg :routing-id
               :initform nil)
   (input-queue :reader input-queue
                :initarg :input-queue
                :initform (make-instance 'queue))
   (peer-lock :accessor peer-lock
              :initform (bt2:make-lock))))

(defmethod remove-peer ((socket socket) peer)
  (loop for k being the hash-keys of (endpoints socket)
          using (hash-value v)
        do (cond ((eq v peer)
                  (remhash k (endpoints socket)))
                 ((typep v 'server)
                  (setf (peers v) (delete peer (peers v)))))))

(defmethod shutdown ((object socket))
  (loop for endpoint being the hash-values of (endpoints object)
        do (usocket:socket-close (handle endpoint))))

(defgeneric handles (object)
  (:method (object)
    (declare (ignore object))
    nil))

(defmethod handles ((client client))
  nil);(list (handle client)))

(defmethod handles ((server server))
  (list (handle server)
        #+(or)(mapcar #'handle (peers server))))

(defun do-accept (server)
  (let ((peer (make-instance 'peer
                             :parent (parent server)
                             :handle (usocket:socket-accept (handle server)))))
    (push peer (peers server))
    (add-peer (parent server) peer)))

(defmethod bind ((socket socket) endpoint)
  (multiple-value-bind (protocol host port resource)
      (parse-endpoint endpoint)
    (declare (ignore protocol))
    (let* ((handle (usocket:socket-listen host port
                                          :element-type '(unsigned-byte 8)
                                          :reuse-address t))
           (endpoint (format nil "tcp://~a:~a~@[/~a~]"
                             host (usocket:get-local-port handle) resource))
           (server (make-instance 'server :handle handle :address endpoint :parent socket)))
      (setf (gethash endpoint (endpoints socket)) server)
      (add-poller (context socket) handle
                  (lambda ()
                    (do-accept server)
                    nil))
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
            (peer endpoint) (make-instance 'peer :handle handle :parent socket))
      (add-peer socket (peer endpoint))
      endpoint)))

(defmethod add-peer :around ((socket socket) peer)
  (declare (ignore peer))
  (bt2:with-lock-held ((peer-lock socket))
    (call-next-method)))

(defmethod add-peer :after (socket peer)
  (add-poller (context socket) (handle peer)
              (lambda () (do-read peer))))

(defmethod remove-peer :around ((socket socket) peer)
  (declare (ignore peer))
  (bt2:with-lock-held ((peer-lock socket))
    (call-next-method)))

(defmethod remove-peer :before (socket peer)
  (remove-poller (context socket) (handle peer)))

(defclass round-robin-socket (socket)
  ((%peers :accessor peers
           :initform nil)))

(defmethod add-peer :after ((socket round-robin-socket) peer)
  (with-accessors ((peers peers))
      socket
    (if peers
        (setf (cdr peers) (cons peer (cddr peers)))
        (setf peers (cons peer nil)
              (cdr peers) peers))))

(defmethod remove-peer :after ((socket round-robin-socket) peer)
  (with-accessors ((peers peers))
      socket
    (cond ((and (eq peer (car peers))
                (eq peers (cdr peers)))
           (setf peers nil))
          (t
           (when (eq (car peers) peer)
             (setf peers (cdr peers)))
           (prog ((head peers))
            repeat
              (unless (eq peer (cadr head))
                (setf head (cdr head))
                (go repeat))
              (setf (cdr head) (cddr head)))))))

(defmethod next-peer ((socket round-robin-socket))
  (setf (peers socket) (cdr (peers socket))))

(defmethod find-peer ((socket round-robin-socket) &optional id)
  (declare (ignore id))
  (car (peers socket)))

(defmethod map-peers ((socket round-robin-socket) func)
  (prog ((head (peers socket)))
   repeat
     (when head
       (funcall func (car hear))
       (setf head (cdr head))
       (unless (eq head (peers socket))
         (go repeat)))))

(defclass address-socket (socket)
  ((%peers :reader peers
           :initform (make-hash-table :test #'equalp))))

(defmethod add-peer :after ((socket address-socket) peer)
  (setf (gethash (routing-id peer) (peers socket))
        peer))

(defmethod remove-peer :after ((socket address-socket) peer)
  (remhash (routing-id peer) (peers socket)))

(defmethod map-peers ((socket address-socket) func)
  (loop for peer being the hash-values of (peers socket)
        do (funcall func peer)))

(defmethod find-peer ((socket address-socket) &optional id)
  (gethash id (peers socket)))

(defclass pool-socket (socket)
  ((%peers :accessor peers
           :initform nil)))

(defmethod add-peer :after ((socket pool-socket) peer)
  (push peer (peers socket)))

(defmethod remove-peer :after ((socket pool-socket) peer)
  (setf (peers socket) (delete peer (peers socket))))

(defmethod map-peers ((socket pool-socket) func)
  (mapc func (peers socket)))

(defclass couple-socket (socket)
  ((%peer :accessor peer
          :initform nil)))

(defmethod add-peer :before ((socket couple-socket) peer)
  (when (peer socket)
    (error "Already have a peer")))

(defmethod add-peer :after ((socket couple-socket) peer)
  (setf (peer socket) peer))

(defmethod remove-peer :after ((socket couple-socket) peer)
  (setf (peer socket) nil))

(defmethod map-peers ((socket couple-socket) func)
  (when (peer socket)
    (funcall func (peer socket))))

(defmethod find-peer ((socket couple-socket) &optional id)
  (peer socket))

(defmethod receive ((socket socket))
  (dequeue (input-queue socket)))

(defmethod input-available-p ((socket socket))
  (not (queue-empty-p (input-queue socket))))

(defmethod add-peer (socket peer)
  (write-greeting (target peer) "NULL" nil)
  (read-greeting (target peer))
  (handshake peer))

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
               (write-uint64 stream size)))
        (write-vstring stream name)
        (send-data stream command data))))
  (finish-output stream))

(defmethod send (stream (message cons))
  (loop for (part . rest) on message
        for (size data) = (multiple-value-list (serialize-data part))
        if (< size 256)
          do (write-byte (if rest (ash 1 +more-bit+) 0) stream)
             (write-byte size stream)
        else
          do (write-byte (logior (ash 1 +long-bit+)
                                 (if rest (ash 1 +more-bit+) 0))
                         stream)
             (write-uint64 stream size)
        do (send-data stream part data)
           (finish-output stream)))

(defmethod receive (socket)
  (let* ((stream (target socket))
         (flags (read-byte stream)))
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

(defmethod handshake ((socket peer))
  (send socket (make-instance 'ready-command
                              :routing-id (routing-id (parent socket))
                              :socket-type (socket-type (parent socket))))
  (let ((response (receive socket)))
    (check-type response ready-command)
    (let ((routing-id (routing-id response)))
      (when routing-id
        (setf (routing-id socket) routing-id)))))
