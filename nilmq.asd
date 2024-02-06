(asdf:defsystem "nilmq"
  :description "Common Lisp ZeroMQ implementation"
  :author "Tarn W. Burton"
  :license "MIT"
  :version "0.1"
  :homepage "https://yitzchak.github.io/nilmq/"
  :bug-tracker "https://github.com/yitzchak/nilmq/issues"
  :depends-on ("bordeaux-threads"
               "closer-mop"
               "nontrivial-gray-streams"
               "usocket")
  :components ((:module "code"
                :serial t
                :components
                ((:file "packages")
                 (:file "interface")
                 (:file "queue")
                 (:file "stream")
                 (:file "context")
                 (:file "commands")
                 (:file "socket")
                 (:file "reqrep")
                 (:file "pubsub")
                 (:file "pipeline")
                 (:file "expair")
                 (:file "channel")))))

(asdf:defsystem "nilmq/test"
  :description "Tests for nilmq"
  :author "Tarn W. Burton"
  :license "MIT"
  :version "0.1"
  :homepage "https://yitzchak.github.io/nilmq/"
  :bug-tracker "https://github.com/yitzchak/nilmq/issues"
  :depends-on ("nilmq"
               "parachute"
               "pzmq")
  :components ((:module "code"
                :pathname #P"code/test/"
                :serial t
                :components
                ((:file "packages")))))
