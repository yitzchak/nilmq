(in-package #:nilmq/test)

(defvar +endpoint+ "tcp://localhost:5555")

(define-test request-response-string.1
  (nilmq:with-context (nil)
    (nilmq:with-socket (sender :req)
      (pzmq:with-socket receiver (:rep)
        (pzmq:bind receiver +endpoint+)
        (let ((endpoint (pzmq:getsockopt receiver :last-endpoint)))
          (nilmq:connect sender endpoint)
          (nilmq:send sender '("ping"))
          (is equal
              "ping"
              (pzmq:recv-string receiver))
          (pzmq:send receiver "pong")
          (is equalp
              '(#(112 111 110 103))
              (nilmq:receive sender)))))))

(define-test request-response-string.2
  (nilmq:with-context (nil)
    (pzmq:with-socket sender (:req)
      (nilmq:with-socket (receiver :rep)
        (let ((endpoint (nilmq:bind receiver +endpoint+)))
          (pzmq:connect sender endpoint)
          (pzmq:send sender "ping")
          (is equalp
              '(#(112 105 110 103))
              (nilmq:receive receiver))
          (nilmq:send receiver '("pong"))
          (is equalp
              "pong"
              (pzmq:recv-string sender)))))))
