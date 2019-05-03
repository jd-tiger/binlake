# binlake-wave.http 

http module is to open http interface for out server to check the followings 
* the sever is alive 
* the delay for each instance 
* dynamic to change the role for dump server 

routers below 


router | introduction | method | parameters | response | example 
---:|:---:|:---:|:---:|:---:|:---
/alive | check the server status | post | NULL | "SUCCESS"/"FAILURE" | curl http://127.0.0.1:8083/alive
/delay | take the delay for each instance | post | NULL | {"127.0.0.1:3306:682648211":1000} |  curl http://127.0.0.1:8083/delay
/kill | kill leader for the specified MySQL host | post | NULL | "SUCCESS"/"FAILURE" |  curl -X POST -d '{"key":"127.0.0.1:3306:682648211"}' http://127.0.0.1:8083/delay

  

