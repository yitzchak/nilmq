(in-package #:nilmq)

(defparameter *context* nil)

(defparameter *default-thread-count* 2)

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
             :initform nil)))

(defmethod context ((instance context))
  instance)

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
