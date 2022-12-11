### Instruction to run

- Run 10 instances of kvs master, kvs worker, Flame master, Flame worker
  - java -cp bin kvs.Master 8000 &
  - java -cp bin flame.FlameMaster 9000 localhost:8000 &
  - java -cp bin kvs.Worker 8001 worker1 localhost:8000 &
  - java -cp bin kvs.Worker 8002 worker2 localhost:8000 &
  - java -cp bin kvs.Worker 8003 worker3 localhost:8000 &
  - java -cp bin kvs.Worker 8004 worker4 localhost:8000 &
  - java -cp bin kvs.Worker 8005 worker5 localhost:8000 &
  - java -cp bin kvs.Worker 8006 worker6 localhost:8000 &
  - java -cp bin kvs.Worker 8007 worker7 localhost:8000 &
  - java -cp bin kvs.Worker 8008 worker8 localhost:8000 &
  - java -cp bin kvs.Worker 8009 worker9 localhost:8000 &
  - java -cp bin kvs.Worker 8010 worker10 localhost:8000 &
  - java -cp bin flame.FlameWorker 9001 localhost:9000 &
  - java -cp bin flame.FlameWorker 9002 localhost:9000 &
  - java -cp bin flame.FlameWorker 9003 localhost:9000 &
  - java -cp bin flame.FlameWorker 9004 localhost:9000 &
  - java -cp bin flame.FlameWorker 9005 localhost:9000 &
  - java -cp bin flame.FlameWorker 9006 localhost:9000 &
  - java -cp bin flame.FlameWorker 9007 localhost:9000 &
  - java -cp bin flame.FlameWorker 9008 localhost:9000 &
  - java -cp bin flame.FlameWorker 9009 localhost:9000 &
  - java -cp bin flame.FlameWorker 9010 localhost:9000 &
- Add blackList table
  - create addTable.jar
  - java -cp bin flame.FlameSubmit localhost:9000 addTable.jar jobs.AddTable blackList pattern
- Then run the crawler job
  - (using a seed url)
    - java -cp bin flame.FlameSubmit localhost:9000 crawler.jar jobs.Crawler http://simple.crawltest.cis5550.net/
  - (using a table where we stopped before)
    - java -cp bin flame.FlameSubmit localhost:9000 crawler.jar jobs.Crawler izdfw1668749294044
- Analytics job
  - Indexer
    - Step 1:
      1. jar -cf indexer.jar bin/jobs/Indexer.class
      2. java -cp bin flame.FlameSubmit localhost:9000 indexer.jar jobs.Indexer
    - Step 2: 
      1. After step 1 is done, you should get an index_imm table, from root folder, run:
        a. ./sortIndex worker1 index_imm.table
        b. ./sortIndex worker2 index_imm.table
        c. ./sortIndex worker3 index_imm.table
        d. ./sortIndex worker4 index_imm.table
        e. ./sortIndex worker5 index_imm.table
    - Step 3: After step 1 is done, you should get sorted_index_imm table under each worker folder, submit the second job:
      1. jar -cf consolidator.jar bin/jobs/Consolidator.class
      2. java -cp bin flame.FlameSubmit localhost:9000 consolidator.jar jobs.Consolidator sorted_index_imm
    - Step 4: Combining several tables from different batch of indexers
      1. Place all indexer tables under one directory (you'll need it for step 3)
      2. Place dict.txt (saved in split_dictionary folder by worker ID) under the same directory
      3. java -cp bin jobs.CombineByKey directory_to_run
      4. This should give you a combined file called "combined.table". If re-run is needed, you have to delete the existing combined table
- Pagerank (1 worker with outdegrees)
  - java -cp bin flame.FlameSubmit localhost:9000 pagerank.jar cis5550.jobs.PageRank 0.01
  - java -cp bin flame.FlameSubmit localhost:9000 pagerank.jar cis5550.jobs.PageRank 0.1 75 (enhanced convergence)
- Backend
  - java -cp bin backend.BackendServer 8080 localhost:8000
- Frontend

  - Run npm install
  - npm start

- Build jar file
  - jar -cf indexer.jar bin/jobs/Indexer.class
  - jar -cf consolidator.jar bin/jobs/Consolidator.class

### TODO
