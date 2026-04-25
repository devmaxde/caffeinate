run:
  cargo run --bin qontext

api:
  QONTEXT_ADDR=127.0.0.1:18080 cargo run -p qontext-api
