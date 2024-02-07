(in-package #:nilmq)

(defparameter *context* nil)

(defparameter *default-thread-count* 3)

(defclass context ()
  ((object-factories :reader object-factories
                     :initform (let ((factories (make-hash-table :test #'equalp)))
                                 (setf (gethash "READY" factories)
                                       (lambda (name size)
                                         (declare (ignore name size))
                                         (make-instance 'ready-command))
                                       (gethash "ERROR" factories)
                                       (lambda (name size)
                                         (declare (ignore name size))
                                         (make-instance 'error-command))
                                       (gethash "SUBSCRIBE" factories)
                                       (lambda (name size)
                                         (declare (ignore name size))
                                         (make-instance 'subscribe-command))
                                       (gethash "CANCEL" factories)
                                       (lambda (name size)
                                         (declare (ignore name size))
                                         (make-instance 'cancel-command)))
                                 factories))
   (%task-queue :accessor task-queue
                :initform (make-instance 'queue))
   (%threads :accessor threads
             :initform nil)
   (%wait-list :accessor wait-list
               :initform nil)
   (%pollers :reader pollers
             :initform (make-hash-table))
   (poller-lock :accessor poller-lock
              :initform (bt2:make-lock))))

(defmethod context ((instance context))
  instance)

(defun run-poller (context)
  (when (wait-list context)
    #+(or)(let ((handles (usocket:wait-for-input (wait-list context) :ready-only t :timeout .1)))
      (when handles
        (bt2:with-lock-held ((poller-lock context))
          (loop with pollers = (pollers context)
                for handle in handles
                for func = (gethash handle pollers)
                when func
                  do (enqueue-task context func)))))
    (usocket:wait-for-input (wait-list context) :timeout .1)
    (bt2:with-lock-held ((poller-lock context))
      (loop for handle being the hash-keys of (pollers context)
              using (hash-value func)
            when (usocket:socket-state handle)
              do (enqueue-task context func)))
    t))

(defun update-pollers (context)
  (let ((requeue (null (wait-list context))))
    (setf (wait-list context)
          (unless (zerop (hash-table-count (pollers context)))
            (usocket:make-wait-list
             (loop for handle being the hash-keys of (pollers context)
                   collect handle))))
    (when requeue
      (enqueue-task context
                    (lambda ()
                      (run-poller context))))))

(defun add-poller (context handle func)
  (bt2:with-lock-held ((poller-lock context))
    (setf (gethash handle (pollers context)) func))
  (update-pollers context))

(defun remove-poller (context handle)
  (bt2:with-lock-held ((poller-lock context))
    (remhash handle (pollers context)))
  (update-pollers context))

(defun default-object-factory (name size)
  (if (numberp name)
      (make-array size :element-type '(unsigned-byte 8))
      (make-instance 'unknown-command :name name)))

(defmethod object-factory ((object context) name)
  (gethash name (object-factories object) #'default-object-factory))

(defmethod object-factory (object name)
  (object-factory (context object) name))

(defmethod (setf object-factory) (function (object context) name)
  (setf (gethash name (object-factories object)) function))

(defmethod (setf object-factory) (function object name)
  (setf (object-factory (context object) name) function))

(defun run-thread-task ()
  (loop for func = (dequeue (task-queue *context*))
        when (funcall func)
          do (enqueue (task-queue *context*) func)))

(defun make-context (&key (thread-count *default-thread-count*))
  (loop with context = (make-instance 'context)
        with specials = `((*context* . ,context))
        repeat thread-count
        finally (return context)
        do (push (bt2:make-thread #'run-thread-task
                                  :initial-bindings specials)
                 (threads context))))

(defclass context-mixin ()
  ((%context :reader context
             :initarg :context)))

(defmethod enqueue-task (instance func)
  (enqueue (task-queue (context instance)) func))
