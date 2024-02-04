(in-package #:nilmq)

(defparameter *context* nil)

(defclass context ()
  ())

(defmethod context ((instance context))
  instance)

(defun make-context ()
  (make-instance 'context))

(defclass context-mixin ()
  ((%context :reader context
             :initarg :context)))
