(in-package #:nilmq)

(defparameter *context* nil)

(defparameter *default-thread-count* 2)

(defclass context ()
  ((%task-queue :accessor task-queue
                :initform (make-instance 'queue))
   (%threads :accessor threads
             :initform nil)))

(defmethod context ((instance context))
  instance)

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
