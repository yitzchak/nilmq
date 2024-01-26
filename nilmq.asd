(asdf:defsystem "nilmq"
  :description "Common Lisp ZeroMQ implementation"
  :author "Tarn W. Burton"
  :license "MIT"
  :version "0.1"
  :homepage "https://yitzchak.github.io/nilmq/"
  :bug-tracker "https://github.com/yitzchak/nilmq/issues"
  :depends-on ("usocket")
  :components ((:module "code"
                :components
                ((:file "packages")
                 (:file "interface")
                 (:file "null-client")))))

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
                :components
                ((:file "packages")))))
