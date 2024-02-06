(in-package #:nilmq)

(defgeneric enqueue (queue item))

(defgeneric dequeue (queue))

(defgeneric queue-empty-p (queue))

(defclass queue ()
  ((head :accessor head
         :initform nil
         :type list)
   (tail :accessor tail
         :initform nil
         :type list)
   (access-lock :accessor access-lock
                :initform (bt2:make-lock))
   (not-empty-condition :accessor not-empty-condition
                        :initform (bt2:make-condition-variable))))

(defmethod enqueue ((queue queue) item)
  (with-accessors ((head head)
                   (tail tail)
                   (access-lock access-lock)
                   (not-empty-condition not-empty-condition))
      queue
    (bt2:with-lock-held (access-lock)
      (let ((old-tail tail))
        (setf tail (list item))
        (cond (old-tail
               (rplacd old-tail tail))
              (t
               (setf head tail)
               (bt2:condition-notify not-empty-condition))))))
  item)

(defmethod dequeue ((queue queue))
  (with-accessors ((head head)
                   (tail tail)
                   (access-lock access-lock)
                   (not-empty-condition not-empty-condition))
      queue
    (bt2:with-lock-held (access-lock)
      (prog ()
       check
         (when head
           (unless (cdr head)
             (setf tail nil))
           (return (pop head)))
         (bt2:condition-wait not-empty-condition access-lock)
         (go check)))))

(defmethod queue-empty-p ((queue queue))
  (with-accessors ((head head)
                   (access-lock access-lock))
      queue
    (bt2:with-lock-held (access-lock)
      (null head))))

(defclass null-queue ()
  ())

(defmethod enqueue ((queue null-queue) item)
  item)

(defmethod dequeue ((queue null-queue))
  nil)

(defmethod queue-empty-p ((queue null-queue))
  t)
