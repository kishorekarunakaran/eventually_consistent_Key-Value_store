# eventually_consistent_Key-Value_store

Command to start replica server:

  ./server.sh {serverName} {port} {path to config file} {configuration value}

Note: Configuration value 1-Read repair, 0-Hinted Handoff

Command to start client:

  ./client.sh {coordinator serverName} {path to config file}

Client is designed to be interactive for get and put requests
Get request syntax: get,key,consistencyLevel
Put request syntax: put,key,value,consistencyLevel

Note: To explicitly stop/kill the replica server(process) on a particular port(Ctrl+C sometimes does not kill process running on that port), use the following Command:

  kill $(lsof -t -i:port)
