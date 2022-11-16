### Instruction to run

- run 4 instances of kvs master, kvs worker, Flame master, Flame worker
  - java -cp bin kvs.Master 8000
  - java -cp bin kvs.Worker 8001 worker1 localhost:8000
  - java -cp bin flame.FlameMaster 9000 localhost:8000
  - java -cp bin flame.FlameWorker 9001 localhost:9000
- then run the job
  - java -cp bin flame.FlameSubmit localhost:9000 crawler.jar jobs.Crawler http://simple.crawltest.cis5550.net/

### TODO

- currently, the kvs needs to be hardcoded in the crawler's flatmap lambda
- the crawler stops after crawling one round (maybe the server thread is too crowded?)
