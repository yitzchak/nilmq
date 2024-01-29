(defpackage #:nilmq
  (:use #:cl)
  (:shadow #:identity)
  (:export #:bind
           #:connect
           #:handshake
           #:name
           #:make-socket
           #:object-factory
           #:receive
           #:receive-data
           #:resource
           #:send
           #:send-data
           #:serialize-data
           #:shutdown
           #:target))
