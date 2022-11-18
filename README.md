### Instruction to run

- run 4 instances of kvs master, kvs worker, Flame master, Flame worker
  - java -cp bin kvs.Master 8000
  - java -cp bin kvs.Worker 8001 worker1 localhost:8000
  - java -cp bin flame.FlameMaster 9000 localhost:8000
  - java -cp bin flame.FlameWorker 9001 localhost:9000
- then run the crawler job
  - (using a seed url)
    - java -cp bin flame.FlameSubmit localhost:9000 crawler.jar jobs.Crawler http://simple.crawltest.cis5550.net/
  - (using a table where we stopped before)
    - java -cp bin flame.FlameSubmit localhost:9000 crawler.jar jobs.Crawler izdfw1668749294044
- analytics job
  - indexer
    - java -cp bin flame.FlameSubmit localhost:9000 indexer.jar jobs.Indexer
  - pagerank
    - java -cp bin flame.FlameSubmit localhost:9000 pagerank.jar cis5550.jobs.PageRank 0.01
    - java -cp bin flame.FlameSubmit localhost:9000 pagerank.jar cis5550.jobs.PageRank 0.1 75 (enhanced convergence)

### TODO

- currently, the kvs needs to be hardcoded in the crawler's flatmap lambda (we can use the table name where it stopped to continue running the crawler)
- not yet enabled the garbage collector, replica and replica maintanence
