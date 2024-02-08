(defpackage #:nilmq
  (:use #:cl)
  (:shadow #:close)
  (:export #:bind
           #:cancel-command
           #:close
           #:connect
           #:destroy-context
           #:destroy-socket
           #:error-command
           #:handshake
           #:input-available-p
           #:make-context
           #:make-socket
           #:name
           #:object-factory
           #:ping-command
           #:pong-command
           #:ready-command
           #:receive
           #:receive-data
           #:resource
           #:routing-id
           #:send
           #:send-data
           #:serialize-data
           #:subscribe-command
           #:target
           #:with-context
           #:with-socket))
