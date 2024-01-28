(in-package #:nilmq)

(defclass queue ()
  ((head :accessor head
         :initform nil
         :type list)
   (tail :accessor tail
         :initform nil
         :type list)
   (access-lock :accessor access-lock
                :initform (bordeaux-threads:make-lock))
   (not-empty-condition :accessor not-empty-condition
                        :initform (bordeaux-threads:make-condition-variable))))

(defun enqueue (queue item)
  (with-accessors ((head head)
                   (tail tail)
                   (access-lock access-lock)
                   (not-empty-condition not-empty-condition))
      queue
    (bordeaux-threads:with-lock-held (access-lock)
      (let ((old-tail tail))
        (setf tail (list item))
        (cond (old-tail
               (rplacd old-tail tail))
              (t
               (setf head tail)
               (bordeaux-threads:condition-notify not-empty-condition)))))))

(defun dequeue (queue)
  (with-accessors ((head head)
                   (tail tail)
                   (access-lock access-lock)
                   (not-empty-condition not-empty-condition))
      queue
    (bordeaux-threads:with-lock-held (access-lock)
      (prog ()
       check
         (when head
           (unless (cdr head)
             (setf tail nil))
           (return (pop head)))
         (bordeaux-threads:condition-wait not-empty-condition access-lock)
         (go check)))))

(defun queue-empty-p (queue)
  (with-accessors ((head head)
                   (access-lock access-lock))
      queue
    (bordeaux-threads:with-lock-held (access-lock)
      (null head))))
