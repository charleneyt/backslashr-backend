### Instruction to run

- Run an instance of kvs.Master and flame.FlameMaster along with 5 kvs.Worker and 5 flame.FlameWorker
  - java -cp bin kvs.Master 8000 
  - java -cp bin flame.FlameMaster 9000 localhost:8000 

  - java -cp bin kvs.Worker 8001 worker1 localhost:8000 
  - java -cp bin kvs.Worker 8002 worker2 localhost:8000 
  - java -cp bin kvs.Worker 8003 worker3 localhost:8000 
  - java -cp bin kvs.Worker 8004 worker4 localhost:8000 
  - java -cp bin kvs.Worker 8005 worker5 localhost:8000 

  - java -cp bin flame.FlameWorker 9001 localhost:9000 
  - java -cp bin flame.FlameWorker 9002 localhost:9000 
  - java -cp bin flame.FlameWorker 9003 localhost:9000 
  - java -cp bin flame.FlameWorker 9004 localhost:9000 
  - java -cp bin flame.FlameWorker 9005 localhost:9000 

- Add blackList table which contains a list of website that we blackListed
  - jar -cf addTable.jar bin/jobs/AddTable.class
  - java -cp bin flame.FlameSubmit localhost:9000 addTable.jar jobs.AddTable blackList pattern

- Crawler job
  - jar -cf crawler.jar bin/jobs/Crawler.class
  - Using a seed url:
    - java -cp bin flame.FlameSubmit localhost:9000 crawler.jar jobs.Crawler http://simple.crawltest.cis5550.net/
  - Using a table where we stopped before:
    - java -cp bin flame.FlameSubmit localhost:9000 crawler.jar jobs.Crawler izdfw1668749294044

- Analytics job
  1. Indexer
    - Step 1:
      1. jar -cf indexer.jar bin/jobs/Indexer.class
      2. java -cp bin flame.FlameSubmit localhost:9000 indexer.jar jobs.Indexer
    - Step 2: After step 1 is done, you should get index_imm table under each worker folder, submit the second job:
      1. jar -cf consolidator.jar bin/jobs/Consolidator.class
      2. java -cp bin flame.FlameSubmit localhost:9000 consolidator.jar jobs.Consolidator index_imm
  2. Pagerank
   	  - jar -cf pagerank.jar bin/jobs/PageRank.class
	  - java -cp bin flame.FlameSubmit localhost:9000 pagerank.jar jobs.PageRank 0.01
	  - java -cp bin flame.FlameSubmit localhost:9000 pagerank.jar jobs.PageRank 0.1 75 (enhanced convergence)

- Backend
  - java -cp bin backend.BackendServer 8080 localhost:8000
  - we used a third-part library json-simple, which is already included as part of the code (downloaded from: https://github.com/fangyidong/json-simple/tree/master/src/main/java/org/json/simple)

- Frontend
  - Run npm install
  - After you run npm install, all the required package should already be download based on package.json
  - If not please download the following packages:
    - npm i antd: https://www.npmjs.com/package/antd
    - npm i shards-react: https://www.npmjs.com/package/shards-react
    - npm i react-dom: https://www.npmjs.com/package/react-dom
    - npm i react-router-dom: https://www.npmjs.com/package/react-router-dom
  - The config file in the package is using the localhost. If you want to use it on EC2, rename the config-EC2 to config
  - npm start
  
 Other third-party resources used:
 1. A list of common English words from Github, filtered for stop words: https://github.com/dwyl/english-words;
